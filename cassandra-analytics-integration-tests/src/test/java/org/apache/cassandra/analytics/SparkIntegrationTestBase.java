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

package org.apache.cassandra.analytics;

import java.util.UUID;

import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Extends the {@link org.apache.cassandra.sidecar.testing.IntegrationTestBase} with Spark functionality for
 * test classes.
 */
public class SparkIntegrationTestBase extends IntegrationTestBase
{
    protected SparkConf sparkConf;
    protected SparkSession sparkSession;

    protected DataFrameReader bulkReaderDataFrame(QualifiedName tableName)
    {
        SparkConf sparkConf = getOrCreateSparkConf();
        SparkSession spark = getOrCreateSparkSession();
        SQLContext sql = spark.sqlContext();
        SparkContext sc = spark.sparkContext();

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors", sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;

        return sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                  .option("sidecar_instances", "localhost,localhost2,localhost3")
                  .option("keyspace", tableName.keyspace()) // unquoted
                  .option("table", tableName.table()) // unquoted
                  .option("DC", "datacenter1")
                  .option("snapshotName", UUID.randomUUID().toString())
                  .option("createSnapshot", "true")
                  .option("defaultParallelism", sc.defaultParallelism())
                  .option("numCores", numCores)
                  .option("sizing", "default")
                  .option("sidecar_port", server.actualPort());
    }

    protected SparkConf getOrCreateSparkConf()
    {
        if (sparkConf == null)
        {
            sparkConf = new SparkConf()
                        .setAppName("Integration test Spark Cassandra Bulk Reader Job")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        // Spark is not case-sensitive by default, but we want to make it case-sensitive for
                        // the quoted identifiers tests where we test mixed case
                        .set("spark.sql.caseSensitive", "True")
                        .set("spark.master", "local[8,4]");
            BulkSparkConf.setupSparkConf(sparkConf, true);
            KryoRegister.setup(sparkConf);
        }
        return sparkConf;
    }

    protected SparkSession getOrCreateSparkSession()
    {
        if (sparkSession == null)
        {
            sparkSession = SparkSession
                           .builder()
                           .config(sparkConf)
                           .getOrCreate();
        }
        return sparkSession;
    }
}
