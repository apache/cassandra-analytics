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

package org.apache.cassandra.spark.example;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * A sample cassandra spark job that writes directly to Cassandra via Sidecar,
 * then reads from Cassandra
 */
public class DirectWriteAndReadJob extends AbstractCassandraJob
{
    public static void main(String[] args)
    {
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
        new DirectWriteAndReadJob().start(args);
    }

    protected JobConfiguration configureJob(SparkContext sc, SparkConf sparkConf)
    {
        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("sidecar_contact_points", "localhost,localhost2,localhost3");
        writeOptions.put("keyspace", "spark_test");
        writeOptions.put("table", "test");
        writeOptions.put("local_dc", "datacenter1");
        writeOptions.put("bulk_writer_cl", "LOCAL_QUORUM");
        writeOptions.put("number_splits", "-1");
        writeOptions.put("data_transport", "DIRECT");

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors",
                                            sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;
        Map<String, String> readerOptions = new HashMap<>();
        readerOptions.put("sidecar_contact_points", "localhost,localhost2,localhost3");
        readerOptions.put("keyspace", "spark_test");
        readerOptions.put("table", "test");
        readerOptions.put("DC", "datacenter1");
        readerOptions.put("snapshotName", UUID.randomUUID().toString());
        readerOptions.put("createSnapshot", "true");
        readerOptions.put("defaultParallelism", String.valueOf(sc.defaultParallelism()));
        readerOptions.put("numCores", String.valueOf(numCores));
        readerOptions.put("sizing", "default");
        return new JobConfiguration(writeOptions, readerOptions);
    }
}
