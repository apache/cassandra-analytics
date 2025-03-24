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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.avro.AvroSchemas;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.Nullable;

/**
 * Recommended implementation of SchemaStore that detects schema changes and regenerates Avro schema.
 * Pass in a `SchemaStorePublisherFactory` to publish the schema downstream.
 */
public class CachingSchemaStore implements SchemaStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CachingSchemaStore.class);
    private final Map<TableIdentifier, SchemaCacheEntry> avroSchemasCache = new ConcurrentHashMap<>();
    //    private final CassandraClusterSchema cassandraClusterSchema;
//    private final SidecarSchema sidecarSchema;
//    private final Vertx vertx;
//    private final CdcConfigImpl cdcConfig;
    @Nullable
    volatile TableSchemaPublisher publisher;
    @Nullable
    volatile CqlToAvroSchemaConverter cqlToAvroSchemaConverter;
    //    private static final CqlToAvroSchemaConverter SCHEMA_CONVERTER = new CqlToAvroSchemaConverter();
    private final SchemaSupplier schemaSupplier;
    private final CassandraVersion cassandraVersion;
    private final SchemaStorePublisherFactory schemaStorePublisherFactory;
    private final CdcOptions cdcOptions;
    private final CqlToAvroSchemaConverter schemaConverter;
//    private final SidecarCdcStats sidecarCdcStats;

    CachingSchemaStore(
//    Vertx vertx,
//                       CassandraClusterSchema cassandraClusterSchema,
//                       CdcConfigImpl cdcConfig,
//                       SidecarCdcStats sidecarCdcStats,
//                       SidecarSchema sidecarSchema
    CassandraVersion cassandraVersion, //TODO: supply version to permit changes in version
    SchemaSupplier schemaSupplier,
    SchemaStorePublisherFactory schemaStorePublisherFactory,
    CdcOptions cdcOptions,
    CqlToAvroSchemaConverter schemaConverter
    )
    {
        super();
        this.cassandraVersion = cassandraVersion;
        this.schemaSupplier = schemaSupplier;
        this.schemaStorePublisherFactory = schemaStorePublisherFactory;
        this.cdcOptions = cdcOptions;
        this.schemaConverter = schemaConverter;
//        this.cassandraClusterSchema = cassandraClusterSchema;
//        this.sidecarSchema = sidecarSchema;
//        this.avroSchemasCache.putAll(createSchemaCache(schemaSupplier.getCdcEnabledTables().get()));
        AvroSchemas.registerLogicalTypes();
//        cassandraClusterSchema.addSchemaChangeListener(this::onSchemaChanged); //FIXME
//        this.vertx = vertx;
//        this.cdcConfig = cdcConfig;
//        this.sidecarCdcStats = sidecarCdcStats;

        final Callable<Void> configChangeCallback = () -> {
            LOGGER.info("Services configuration changed. Reloading publisher...");
            publishSchemas();
            return null;
        };
//        this.cdcConfig.registerConfigChangeListener(configChangeCallback); //FIXME
        configureSidecarServerEventListeners();
    }

    private TableSchemaPublisher publisher()
    {
        if (this.publisher == null)
        {
            this.publisher = schemaStorePublisherFactory.buildPublisher(cdcOptions); //SchemaStorePublisherFactory.getFromCdcConfig(cdcConfig);
        }
        return this.publisher;
    }

    private void configureSidecarServerEventListeners()
    {
        //FIXME
//        EventBus eventBus = vertx.eventBus();
//
//        eventBus.localConsumer(ON_SERVER_START.address(), startMessage -> {
//            eventBus.localConsumer(ON_SIDECAR_SCHEMA_INITIALIZED.address(), message -> {
//                LOGGER.debug("Sidecar Schema initialized message={}", message);
//                Set<CqlTable> refreshedCdcTables = cassandraClusterSchema.getCdcTables();
//                for (CqlTable cqlTable : refreshedCdcTables)
//                {
//                    TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
//                    avroSchemasCache.compute(tableIdentifier, (k, v) ->
//                    {
//                        cdcDatabaseAccessor.insertTableSchemaHistory(cqlTable.keyspace(), cqlTable.table(), cqlTable.createStmt());
//                        return v;
//                    });
//                }
//                loadPublisher();
//                publishSchemas();
//            });
//        });
    }

    void onStart()
    {
        LOGGER.debug("CachingSchemaStore initialized");
        schemaSupplier.getCdcEnabledTables();
        schemaSupplier
        .getCdcEnabledTables()
        .thenAccept(refreshedCdcTables -> {
            for (CqlTable cqlTable : refreshedCdcTables)
            {
                TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                avroSchemasCache.compute(tableIdentifier, (k, v) -> v);
            }
            publishSchemas();
        });
    }

//    protected CqlToAvroSchemaConverter schemaConverter()
//    {
//        if (cqlToAvroSchemaConverter == null)
//        {
//            cqlToAvroSchemaConverter = (CqlToAvroSchemaConverter) CdcBridgeFactory.getAvroConverterRequired(cassandraVersion);
//        }
//        return cqlToAvroSchemaConverter;
//    }

    private void publishSchemas()
    {
        schemaSupplier
        .getCdcEnabledTables()
        .thenAccept(refreshedCdcTables -> {
            for (CqlTable cqlTable : refreshedCdcTables)
            {
                TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                avroSchemasCache.compute(tableIdentifier, (key, value) -> {
                    Schema schema = schemaConverter.convert(cqlTable);
                    TableSchemaPublisher publisherRef = publisher();
                    if (publisherRef != null)
                    {
                        TableSchemaPublisher.SchemaPublishMetadata metadata = new TableSchemaPublisher.SchemaPublishMetadata();
                        metadata.put("name", cqlTable.table());
                        metadata.put("namespace", cqlTable.keyspace());
                        publisherRef.publishSchema(schema.toString(), metadata);
//                    sidecarCdcStats.capturePublishedSchema(); //FIXME
                    }
                    return new SchemaCacheEntry(schema, cqlTable);
                });
            }
        }).whenComplete((aVoid, throwable) -> {
            if (throwable != null)
            {
                LOGGER.warn("Failed to ");
            }
        });
//        Set<CqlTable> refreshedCdcTables = schemaSupplier.getCdcEnabledTables();
    }

    void onSchemaChanged()
    {
        schemaSupplier
        .getCdcEnabledTables()
        .thenAccept(refreshedCdcTables -> {
            for (CqlTable cqlTable : refreshedCdcTables)
            {
                TableIdentifier tableIdentifier = TableIdentifier.of(cqlTable.keyspace(), cqlTable.table());
                avroSchemasCache.compute(tableIdentifier, (key, value) -> {
                    if (value == null || !value.tableSchema().equals(cqlTable.createStatement()))
                    {
                        LOGGER.info("Re-generating Avro Schema after schema change keyspace={} table={}", tableIdentifier.keyspace(), tableIdentifier.table());
                        return new SchemaCacheEntry(schemaConverter.convert(cqlTable), cqlTable);
                    }
                    return value;
                });
                publishSchemas();
            }
            // Remove any old schema entries for deleted tables, this operation can be done in the end as this is
            // only for removing stale entries and no one is going to use these entries once the table is removed.
            // This doesn't have to be an atomic operation.
            avroSchemasCache
            .keySet()
            .retainAll(refreshedCdcTables
                       .stream()
                       .map(cqlTable -> TableIdentifier.of(cqlTable.keyspace(), cqlTable.table()))
                       .collect(Collectors.toList()));
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
        return avroSchemasCache.values().stream()
                               .collect(Collectors.toMap(e -> e.schema.getNamespace(), e -> e.schema));
    }

    private Map<TableIdentifier, SchemaCacheEntry> createSchemaCache(Set<CqlTable> cdcTables)
    {
        return cdcTables
               .stream()
               .collect(Collectors.toMap(
               CqlTable::tableIdentifier,
               table -> new SchemaCacheEntry(schemaConverter.convert(table), table)
               ));
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
