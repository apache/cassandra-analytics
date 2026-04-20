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

package org.apache.cassandra.cdc.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.generic.GenericData;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.CdcLogMode;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.Preconditions;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;

/**
 * Publishes CDC events to Kafka.
 *
 * <p>Each subclass holds its own {@link org.apache.cassandra.cdc.CdcEventTransformer} and implements
 * {@link #getPayload} to convert a {@link CdcEvent} into the value type {@code V}.
 * The deterministic UUID-v3 of the schema is stamped as the {@code schemaUuid} Kafka header
 * via {@link org.apache.cassandra.cdc.schemastore.SchemaStore#getVersion}.
 *
 * <p>The type parameter {@code V} is the value type accepted by the Kafka producer:
 * {@code V = GenericData.Record} for PIE / Confluent paths (the serializer handles encoding),
 * or {@code V = byte[]} for the no-registry path — see {@link ByteArrayKafkaPublisher}.
 *
 * @param <V> the Kafka producer value type
 */
public abstract class KafkaPublisher<V> implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisher.class);

    protected CassandraVersion version;
    protected TopicSupplier topicSupplier;
    protected int maxRecordSizeBytes;
    protected final RecordProducer<V> recordProducer;
    protected final EventHasher eventHasher;
    protected boolean failOnRecordTooLargeError;
    protected boolean failOnKafkaError;
    protected CdcLogMode cdcLogMode;

    protected final AtomicReference<Throwable> failure = new AtomicReference<>();
    protected final KafkaProducer<String, V> producer;
    private final SchemaStore schemaStore;

    protected ThreadLocal<Map<Pair<String, String>, String>> prefixCache =
    ThreadLocal.withInitial(HashMap::new);
    protected final KafkaStats kafkaStats;

    KafkaPublisher(CassandraVersion version,
                   TopicSupplier topicSupplier,
                   KafkaProducer<String, V> producer,
                   SchemaStore schemaStore,
                   int maxRecordSizeBytes,
                   boolean failOnRecordTooLargeError,
                   boolean failOnKafkaError,
                   CdcLogMode logMode)
    {
        this(
        version,
        topicSupplier,
        producer,
        schemaStore,
        maxRecordSizeBytes,
        failOnRecordTooLargeError,
        failOnKafkaError,
        logMode,
        KafkaStats.STUB,
        RecordProducer.defaultProducer(),
        EventHasher.MURMUR2
        );
    }

    KafkaPublisher(CassandraVersion version,
                   TopicSupplier topicSupplier,
                   KafkaProducer<String, V> producer,
                   SchemaStore schemaStore,
                   int maxRecordSizeBytes,
                   boolean failOnRecordTooLargeError,
                   boolean failOnKafkaError,
                   CdcLogMode logMode,
                   KafkaStats kafkaStats,
                   RecordProducer<V> recordProducer,
                   EventHasher eventHasher)
    {
        this.version = version;
        this.topicSupplier = topicSupplier;
        this.maxRecordSizeBytes = maxRecordSizeBytes;
        this.failOnRecordTooLargeError = failOnRecordTooLargeError;
        this.failOnKafkaError = failOnKafkaError;
        this.cdcLogMode = logMode;
        this.kafkaStats = kafkaStats;
        this.schemaStore = schemaStore;
        this.producer = producer;
        this.eventHasher = eventHasher;
        this.recordProducer = recordProducer;
        CdcLogMode.init(this::getType);
        kafkaStats.registerKafkaPublishErrorKpi();
    }

    public CqlField.CqlType getType(KeyspaceTypeKey key)
    {
        return TypeCache.get(version()).getType(key.keyspace, key.type);
    }

    public CassandraVersion version()
    {
        return version;
    }

    public Logger logger()
    {
        // permits user to override with their own logger if they wish
        return LOGGER;
    }

    protected RecordProducer<V> recordProducer()
    {
        return recordProducer;
    }

    /**
     * Converts the transformed {@link GenericData.Record} into the value type {@code V}
     * expected by the Kafka producer.
     *
     * <p>The default implementation is an identity cast for PIE / Confluent paths
     * where {@code V = GenericData.Record}. Override to perform encoding, e.g. in
     * {@link ByteArrayKafkaPublisher} for the {@code ByteArraySerializer} path.
     */
    protected abstract V getPayload(CdcEvent cdcEvent);

    public void processEvent(CdcEvent event)
    {
        String topic = topicSupplier.topic(event);
        cdcLogMode.info(logger(), "Processing CDC event", event, topic);
        long time = System.currentTimeMillis();
        String schemaUuid = schemaStore.getVersion(event.keyspace + "." + event.table, null);
        String publishKey = getOrBuildKafkaPrefix(event) + eventHasher.hashEvent(event);
        V payload;
        try
        {
            payload = getPayload(event);
        }
        catch (Exception e)
        {
            cdcLogMode.warn(logger(), "Skip publishing the event because it cannot be serialized",
                            event, topic, e);
            throw e;
        }
        List<ProducerRecord<String, V>> records = recordProducer()
                                                  .buildRecords(event, topic, publishKey, payload, schemaUuid);
        for (ProducerRecord<String, V> producerRecord : records)
        {
            producer.send(producerRecord, (metadata, throwable) -> {
                long elapsedTime = System.currentTimeMillis() - time;
                if (throwable != null)
                {
                    kafkaStats.reportKafkaPublishError();
                    if (throwable instanceof RecordTooLargeException)
                    {
                        kafkaStats.reportKafkaRecordTooLarge();
                        cdcLogMode.error(logger(), "Kafka record too large exception", event, topic,
                                         throwable);
                        if (failOnRecordTooLargeError)
                        {
                            failure.compareAndSet(null, throwable);
                        }
                    }
                    else
                    {
                        cdcLogMode.error(logger(), "Error publishing record to Kafka", event, topic,
                                         throwable);
                        if (failOnKafkaError)
                        {
                            failure.compareAndSet(null, throwable);
                        }
                    }
                }
                else
                {
                    kafkaStats.changePublished(event);
                    logger().debug(
                    "Sent record(topic={}) meta(partition={}, offset={}) time={} topic={}",
                    topic, metadata.partition(), metadata.offset(), elapsedTime, topic);
                }
            });
        }
    }

    @Override
    public void close()
    {
        close(null);
    }

    public void flush() throws InterruptedException
    {
        KafkaProducer<String, V> producerRef = this.producer;
        if (producerRef == null)
        {
            return;
        }

        try
        {
            producerRef.flush();

            // if we get failures after flushing, then report back to the caller so cdc state is not persisted
            Throwable t = failure.get();
            if (t != null && failure.compareAndSet(t, null))
            {
                kafkaStats.reportJobFailure();
                throw new RuntimeException(t);
            }
        }
        catch (InterruptException e)
        {
            Thread.currentThread().interrupt();
            throw new java.lang.InterruptedException();
        }
    }

    public void close(Throwable t)
    {
        if (t != null)
        {
            logger().error("Unexpected exception streaming rows", t);
        }
        else
        {
            logger().info("Closing streaming job");
        }

        if (producer != null)
        {
            producer.close();
        }
    }

    protected String getOrBuildKafkaPrefix(CdcEvent event)
    {
        return prefixCache
               .get()
               .computeIfAbsent(
               Pair.of(event.keyspace, event.table),
               args -> String.format("%s:%s:", event.keyspace, event.table)
               );
    }

    public static TableIdentifier extractTableIdFromPublishKey(String publishKey)
    {
        String[] components = publishKey.split(":");
        // see getOrBuildKafkaPrefix(), the TableIdentifier is the prefix
        Preconditions.checkArgument(components.length == 3);
        return TableIdentifier.of(components[0], components[1]);
    }
}
