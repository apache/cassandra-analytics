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

package org.apache.cassandra.spark.bulkwriter;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraCoordinatedBulkWriterContext;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.ShutdownHookManager;
import org.jetbrains.annotations.NotNull;

public class BulkWriterContextFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkWriterContextFactory.class);

    @NotNull
    public BulkWriterContext createBulkWriterContext(@NotNull SparkContext sparkContext,
                                                     @NotNull Map<String, String> options,
                                                     @NotNull StructType schema)
    {
        Preconditions.checkNotNull(schema);

        BulkSparkConf conf = createBulkSparkConf(sparkContext, options);
        int sparkDefaultParallelism = sparkContext.defaultParallelism();
        BulkWriterContext bulkWriterContext;
        if (conf.isCoordinatedWriteConfigured())
        {
            LOGGER.info("Initializing bulk writer context for multi-clusters coordinated write");
            bulkWriterContext = createCoordinatedBulkWriterContext(conf, schema, sparkDefaultParallelism);
        }
        else
        {
            LOGGER.info("Initializing bulk writer context for single cluster write");
            bulkWriterContext = createBulkWriterContext(conf, schema, sparkDefaultParallelism);
        }

        ShutdownHookManager.addShutdownHook(org.apache.spark.util.ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY(),
                                            ScalaFunctions.wrapLambda(bulkWriterContext::shutdown));
        publishInitialJobStats(bulkWriterContext, sparkContext.version());
        return bulkWriterContext;
    }

    @NotNull
    protected BulkSparkConf createBulkSparkConf(@NotNull SparkContext sparkContext, @NotNull Map<String, String> options)
    {
        return new BulkSparkConf(sparkContext.getConf(), options);
    }

    @NotNull
    protected BulkWriterContext createBulkWriterContext(@NotNull BulkSparkConf conf, StructType schema, int sparkDefaultParallelism)
    {
        return new CassandraBulkWriterContext(conf, schema, sparkDefaultParallelism);
    }

    @NotNull
    protected BulkWriterContext createCoordinatedBulkWriterContext(@NotNull BulkSparkConf conf, StructType schema, int sparkDefaultParallelism)
    {
        return new CassandraCoordinatedBulkWriterContext(conf, schema, sparkDefaultParallelism);
    }

    protected void publishInitialJobStats(BulkWriterContext context, String sparkVersion)
    {
        JobInfo jobInfo = context.job();
        Map<String, String> initialJobStats = new HashMap<String, String>() // type declaration required to compile with java8
        {{
            put("jobId", jobInfo.getId());
            put("sparkVersion", sparkVersion);
            put("keyspace", jobInfo.qualifiedTableName().keyspace());
            put("table", jobInfo.qualifiedTableName().table());
        }};
        context.jobStats().publish(initialJobStats);
    }
}
