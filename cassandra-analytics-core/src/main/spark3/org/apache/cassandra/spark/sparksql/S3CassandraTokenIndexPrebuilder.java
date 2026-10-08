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

package org.apache.cassandra.spark.sparksql;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.S3CassandraDataLayer;
import org.apache.cassandra.spark.data.S3DataSourceClientConfig;
import org.apache.cassandra.spark.data.SSTableSummaryWorkItem;
import org.apache.cassandra.spark.data.SSTableTokenIndex;
import org.apache.cassandra.spark.data.SSTableTokenIndexBuilder;
import org.apache.cassandra.spark.data.TokenIndexShard;
import org.apache.cassandra.spark.data.backup.BackupReaderConfig;
import org.apache.cassandra.spark.data.backup.BackupReaderFactory;
import org.apache.cassandra.spark.data.backup.BackupReaderRegistry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.Nullable;

public final class S3CassandraTokenIndexPrebuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3CassandraTokenIndexPrebuilder.class);

    private S3CassandraTokenIndexPrebuilder()
    {
    }

    public static S3CassandraPrebuiltReadContext prepare(SparkSession sparkSession, Map<String, String> options)
    {
        return prepare(sparkSession, new CaseInsensitiveStringMap(options));
    }

    public static S3CassandraPrebuiltReadContext prepare(SparkSession sparkSession, CaseInsensitiveStringMap options)
    {
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);
        S3CassandraDataLayer dataLayer = new S3CassandraDataLayer(config);
        Broadcast<SSTableTokenIndex> broadcast = dataLayer.sstableTokenIndexEnabled()
                                                 ? buildBroadcast(sparkSession, dataLayer)
                                                 : null;
        String id = UUID.randomUUID().toString();
        S3CassandraPrebuiltReadContext context = new S3CassandraPrebuiltReadContext(id, dataLayer, broadcast);
        S3CassandraPrebuiltReadContextRegistry.register(context);
        LOGGER.info("Registered S3 Cassandra prebuilt read context id={} tokenIndexEnabled={} hasBroadcast={}",
                    id, dataLayer.sstableTokenIndexEnabled(), broadcast != null);
        return context;
    }

    @Nullable
    private static Broadcast<SSTableTokenIndex> buildBroadcast(SparkSession sparkSession, S3CassandraDataLayer dataLayer)
    {
        long startNanos = System.nanoTime();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        List<SSTableSummaryWorkItem> workItems = dataLayer.sstableTokenIndexWorkItems();
        int partitionCount = dataLayer.sstableTokenIndexPrebuildPartitions(sparkContext.defaultParallelism());
        int concurrency = dataLayer.sstableTokenIndexPrebuildPerTaskConcurrency();
        // Capture the factory + a stats-less config into the Spark closure. Executors instantiate
        // the reader via the factory; stats are reinstalled inside buildShard.
        BackupReaderFactory backupReaderFactory = BackupReaderRegistry.factoryFor(dataLayer.backupReaderType());
        BackupReaderConfig backupReaderConfig = BackupReaderConfig.of(dataLayer.s3ClientConfig());
        String clusterName = dataLayer.clusterName();
        String datacenter = dataLayer.datacenter();
        CassandraVersion cassandraVersion = dataLayer.version();
        LOGGER.info("Building SSTable token index sstableCount={} prebuildPartitions={} perTaskConcurrency={} backupReaderType={}",
                    workItems.size(), partitionCount, concurrency, dataLayer.backupReaderType());

        List<TokenIndexShard> shards = sparkContext.parallelize(workItems, partitionCount)
                                                   .mapPartitions(items -> Collections.singletonList(
                                                       SSTableTokenIndexBuilder.buildShard(items,
                                                                                           backupReaderFactory,
                                                                                           backupReaderConfig,
                                                                                           clusterName,
                                                                                           datacenter,
                                                                                           cassandraVersion,
                                                                                           concurrency)).iterator())
                                                   .collect();
        SSTableTokenIndex tokenIndex = SSTableTokenIndex.fromShards(shards);
        Broadcast<SSTableTokenIndex> broadcast = sparkContext.broadcast(tokenIndex);

        long elapsedNanos = System.nanoTime() - startNanos;
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0D;
        double summariesPerSecond = elapsedSeconds == 0 ? workItems.size() : workItems.size() / elapsedSeconds;
        LOGGER.info("Built SSTable token index sstableCount={} indexed={} missing={} errors={} "
                    + "elapsedSeconds={} summariesPerSecond={} estimatedBroadcastBytes={}",
                    workItems.size(), tokenIndex.successCount(), tokenIndex.missingCount(), tokenIndex.errorCount(),
                    elapsedSeconds, summariesPerSecond, tokenIndex.estimatedSizeInBytes());
        return broadcast;
    }
}
