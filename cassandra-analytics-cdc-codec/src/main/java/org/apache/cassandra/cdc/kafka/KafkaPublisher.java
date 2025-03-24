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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.kafka.common.serialization.Serializer;

public class KafkaPublisher implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisher.class);

    protected TopicSupplier topicSupplier;
    protected int maxRecordSizeBytes;
    protected final RecordProducer recordProducer;
    protected final EventHasher eventHasher;
    protected boolean failOnRecordTooLargeError;
    protected boolean failOnKafkaError;
    protected CdcLogMode cdcLogMode;

    protected final AtomicReference<Throwable> failure = new AtomicReference<>();
    protected final KafkaProducer<String, byte[]> producer;
    protected final Serializer<CdcEvent> serializer;

    protected ThreadLocal<Map<Pair<String, String>, String>> prefixCache = ThreadLocal.withInitial(HashMap::new);
    protected final KafkaStats kafkaStats;

    public KafkaPublisher(TopicSupplier topicSupplier,
                          KafkaProducer<String, byte[]> producer,
                          Serializer<CdcEvent> serializer,
                          int maxRecordSizeBytes,
                          boolean failOnRecordTooLargeError,
                          boolean failOnKafkaError,
                          CdcLogMode logMode)
    {
        this(
        topicSupplier,
        producer,
        serializer,
        maxRecordSizeBytes,
        failOnRecordTooLargeError,
        failOnKafkaError,
        logMode,
        KafkaStats.STUB,
        RecordProducer.DEFAULT,
        EventHasher.MURMUR3
        );
    }

    public KafkaPublisher(TopicSupplier topicSupplier,
                          KafkaProducer<String, byte[]> producer,
                          Serializer<CdcEvent> serializer,
                          int maxRecordSizeBytes,
                          boolean failOnRecordTooLargeError,
                          boolean failOnKafkaError,
                          CdcLogMode logMode,
                          KafkaStats kafkaStats,
                          RecordProducer recordProducer,
                          EventHasher eventHasher)
    {
        this.topicSupplier = topicSupplier;
        this.maxRecordSizeBytes = maxRecordSizeBytes;
        this.failOnRecordTooLargeError = failOnRecordTooLargeError;
        this.failOnKafkaError = failOnKafkaError;
        this.cdcLogMode = logMode;
        this.kafkaStats = kafkaStats;
        this.serializer = serializer;
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
        return CassandraVersion.FOURZERO;
    }

    public Logger logger()
    {
        //
        return LOGGER;
    }

    protected RecordProducer recordProducer()
    {
        return recordProducer;
    }

    protected byte[] getPayload(String topic, CdcEvent event)
    {
        return serializer.serialize(topic, event);
    }

    public void processEvent(CdcEvent event)
    {
        final String topic = topicSupplier.topic(event);
        cdcLogMode.info(logger(), "Processing CDC event", event, topic);
        long time = System.currentTimeMillis();
        final byte[] recordPayload = getPayload(topic, event);
        if (recordPayload == null)
        {
            cdcLogMode.warn(logger(), "Skip publishing the event because it cannot be serialized", event, topic, null);
            return;
        }
        final String publishKey = getOrBuildKafkaPrefix(event) + eventHasher.hashEvent(event);
        final List<ProducerRecord<String, byte[]>> records = recordProducer()
                                                             .buildRecords(event, topic, publishKey, recordPayload);
        for (ProducerRecord<String, byte[]> record : records)
        {
            producer.send(record, (metadata, throwable) -> {
                long elapsedTime = System.currentTimeMillis() - time;
                if (throwable != null)
                {
                    kafkaStats.reportKafkaPublishError();
                    if (throwable instanceof RecordTooLargeException)
                    {
                        kafkaStats.reportKafkaRecordTooLarge();
                        cdcLogMode.error(logger(), "Kafka record too large exception", event, topic, throwable);
                        if (failOnRecordTooLargeError)
                        {
                            failure.compareAndSet(null, throwable);
                        }
                    }
                    else
                    {
                        cdcLogMode.error(logger(), "Error publishing record to Kafka", event, topic, throwable);
                        if (failOnKafkaError)
                        {
                            failure.compareAndSet(null, throwable);
                        }
                    }
                }
                else
                {
                    kafkaStats.changePublished(event);
                    logger().debug("Sent record(topic={}) meta(partition={}, offset={}) time={} topic={}",
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
        KafkaProducer<String, byte[]> producerRef = this.producer;
        if (producerRef == null)
        {
            return;
        }

        try
        {
            producerRef.flush();

            // if we get failures after flushing, then report back to the caller so cdc state is not persisted
            final Throwable t = failure.get();
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

        if (serializer != null)
        {
            serializer.close();
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
