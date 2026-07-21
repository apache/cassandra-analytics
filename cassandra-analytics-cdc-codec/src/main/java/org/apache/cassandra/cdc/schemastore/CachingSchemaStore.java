/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.cdc.schemastore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.cdc.kafka.KafkaOptions;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Recommended implementation of SchemaStore that detects schema changes and regenerates Avro schema.
 * Pass in a `SchemaStorePublisherFactory` to publish the schema downstream.
 */
public class CachingSchemaStore implements SchemaStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingSchemaStore.class);
    private final Map<TableIdentifier, SchemaCacheEntry> avroSchemasCache = new ConcurrentHashMap<>();
    @Nullable
    volatile TableSchemaPublisher publisher;
    private final SchemaSupplier schemaSupplier;
    private final Supplier<CassandraVersion> cassandraVersionSupplier;
    private final SchemaStorePublisherFactory schemaStorePublisherFactory;
    private final KafkaOptions kafkaOptions;
    private final SchemaStoreStats schemaStoreStats;

    public CachingSchemaStore(SchemaStoreStats schemaStoreStats,
                              Supplier<CassandraVersion> cassandraVersionSupplier,
                              SchemaSupplier schemaSupplier,
                              SchemaStorePublisherFactory schemaStorePublisherFactory,
                              KafkaOptions kafkaOptions)
    {
        this.cassandraVersionSupplier = cassandraVersionSupplier;
        this.schemaSupplier = schemaSupplier;
        this.schemaStorePublisherFactory = schemaStorePublisherFactory;
        this.kafkaOptions = kafkaOptions;
        this.schemaStoreStats = schemaStoreStats;
        AvroSchemas.registerLogicalTypes();
    }

    /**
     * `initialize()` must be called on server start-up once all other dependencies are initialized,
     * e.g. when Sidecar has fully initialized connections to Cassandra.
     */
    public void initialize()
    {
        LOGGER.info("Initializing CachingSchemaStore");
        schemaSupplier.getTables()
                      .thenAccept(ignored -> {
                          loadPublisher();
                          publishSchemas();
                          LOGGER.info("CachingSchemaStore initialized");
                      });
    }

    /**
     * `onConfigChange()` should be called whenever the Kafka config is changed and the publisher needs to be rebuilt.
     */
    public void onConfigChange()
    {
        LOGGER.info("Services configuration changed. Reloading publisher...");
        loadPublisher();
        publishSchemas();
    }

    /**
     * `onSchemaChanged()` should be called whenever a Cassandra CQL schema change is detected.
     */
    public void onSchemaChange()
    {
        schemaSupplier.getCDCEnabledTables().thenAccept(refreshedCdcTables -> {
            for (CqlTable cqlTable : refreshedCdcTables)
            {
                TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                avroSchemasCache.compute(tableIdentifier, (key, value) -> {
                    if (value == null || !value.tableSchema().equals(cqlTable.createStatement()))
                    {
                        LOGGER.info("Re-generating Avro Schema after schema change keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
                        return new SchemaCacheEntry(schemaConverter().convert(cqlTable), cqlTable);
                    }
                    return value;
                });
            }
            // publishSchemas() re-fetches and republishes every CDC table's schema, so it only
            // needs to run once after the cache has been updated for all changed tables above —
            // calling it per-table would redundantly re-fetch the schema and republish every
            // table N times over.
            if (!refreshedCdcTables.isEmpty())
            {
                publishSchemas();
            }
            else
            {
                LOGGER.warn("No CDC-enabled tables found; no schemas will be published until CDC is enabled on a table");
            }
            // Remove any old schema entries for deleted tables
            List<TableIdentifier> refreshedTableIds = refreshedCdcTables
                                                      .stream()
                                                      .map(cqlTable -> TableIdentifier.of(cqlTable.keyspace(), cqlTable.table()))
                                                      .collect(Collectors.toList());
            avroSchemasCache.keySet().retainAll(refreshedTableIds);
        });
    }

    private synchronized void loadPublisher()
    {
        TableSchemaPublisher publisherRef = this.publisher;
        if (publisherRef != null)
        {
            try
            {
                publisherRef.close();
            }
            catch (Exception exception)
            {
                LOGGER.warn("Failed to shut down schema publisher", exception);
            }
        }
        this.publisher = schemaStorePublisherFactory.buildPublisher(kafkaOptions);
    }

    @NotNull
    protected CqlToAvroSchemaConverter schemaConverter()
    {
        return Objects.requireNonNull(
        CdcBridgeFactory.getCqlToAvroSchemaConverter(cassandraVersionSupplier.get()),
        "CqlToAvroSchemaConverter could not be found by the CdcBridgeFactory"
        );
    }

    private void publishSchemas()
    {
        schemaSupplier
        .getCDCEnabledTables()
        .thenAccept(refreshedCdcTables -> {
            for (CqlTable cqlTable : refreshedCdcTables)
            {
                TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                avroSchemasCache.compute(tableIdentifier, (key, value) -> {
                    Schema schema = schemaConverter().convert(cqlTable);
                    TableSchemaPublisher publisherRef = this.publisher;
                    if (publisherRef != null)
                    {
                        TableSchemaPublisher.SchemaPublishMetadata metadata = new TableSchemaPublisher.SchemaPublishMetadata();
                        metadata.put("name", cqlTable.table());
                        metadata.put("namespace", cqlTable.keyspace());
                        publisherRef.publishSchema(schema.toString(), metadata);
                        schemaStoreStats.capturePublishedSchema();
                    }
                    return new SchemaCacheEntry(schema, cqlTable);
                });
            }
        }).whenComplete((aVoid, throwable) -> {
            if (throwable != null)
            {
                LOGGER.warn("Failed to publish Avro schemas", throwable);
            }
        });
    }

    @Override
    public Schema getSchema(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting schema keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get schema for unknown table " + tableIdentifier);
        }).schema;
    }

    @Override
    public GenericDatumWriter<GenericRecord> getWriter(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting writer keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get writer for unknown table " + tableIdentifier);
        }).writer;
    }

    @Override
    public GenericDatumReader<GenericRecord> getReader(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting reader keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get reader for unknown table " + tableIdentifier);
        }).reader;
    }

    @Override
    public String getVersion(String namespace, String name)
    {
        TableIdentifier tableIdentifier = getTableIdentifierFromNamespace(namespace);
        return avroSchemasCache.computeIfAbsent(tableIdentifier, k -> {
            LOGGER.warn("Unknown table for getting reader keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
            throw new RuntimeException("Unable to get reader for unknown table " + tableIdentifier);
        }).schemaUuid;
    }

    public Map<String, Schema> getSchemas()
    {
        return avroSchemasCache
               .values()
               .stream()
               .collect(Collectors.toMap(entry -> entry.schema.getNamespace(), entry -> entry.schema));
    }

    private TableIdentifier getTableIdentifierFromNamespace(String namespace)
    {
        String[] namespaceParts = namespace.split("\\.");
        return TableIdentifier.of(namespaceParts[0], namespaceParts[1]);
    }

    private static class SchemaCacheEntry
    {
        private final CqlTable table;
        private final Schema schema;
        private final String schemaUuid;
        private final GenericDatumWriter<GenericRecord> writer;
        private final GenericDatumReader<GenericRecord> reader;

        private SchemaCacheEntry(Schema schema, CqlTable table)
        {
            this.table = table;
            this.schema = schema;
            this.schemaUuid = UUID.nameUUIDFromBytes(table.createStatement().getBytes(StandardCharsets.UTF_8)).toString();
            this.writer = new GenericDatumWriter<>(schema);
            this.reader = new GenericDatumReader<>(schema);
        }

        public String tableSchema()
        {
            return table.createStatement();
        }
    }
}
