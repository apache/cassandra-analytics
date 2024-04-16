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

import static org.apache.cassandra.spark.example.LocalStorageTransportExtension.BUCKET_NAME;

/**
 * A sample Cassandra spark job that writes to (local) s3 first and imports into Cassandra via Sidecar
 */
public class LocalS3CassandraWriteJob extends AbstractCassandraJob
{
    private String dataCenter = "datacenter1";
    private String sidecarInstances = "localhost,localhost2,localhost3";

    LocalS3CassandraWriteJob(String[] args)
    {
        if (args.length == 2)
        {
            dataCenter = args[0];
            sidecarInstances = args[1];
        }
    }

    public static void main(String[] args)
    {
        // It expects to have mocks3 running locally on 9090
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("curl", "-X", "PUT", "localhost:9090/" + BUCKET_NAME);
        try
        {
            pb.start().waitFor();
        }
        catch (Exception e)
        {
            // ignore when the bucket is already created
        }

        new LocalS3CassandraWriteJob(args).start(args);
    }

    protected JobConfiguration configureJob(SparkContext sc, SparkConf sparkConf)
    {
        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("sidecar_instances", sidecarInstances);
        writeOptions.put("keyspace", "spark_test");
        writeOptions.put("table", "test");
        writeOptions.put("local_dc", dataCenter);
        writeOptions.put("bulk_writer_cl", "LOCAL_QUORUM");
        writeOptions.put("number_splits", "-1");
        // ---- Below write options are for S3_COMPAT impl only ----
        // Set the data transport mode to "S3_COMPAT" to use an AWS S3-compatible
        // storage service to move data from Spark to Sidecar
        writeOptions.put("data_transport", "S3_COMPAT");
        writeOptions.put("data_transport_extension_class", LocalStorageTransportExtension.class.getCanonicalName());

        // It is only needed in order to talk to the local mocks3 server. Do not set the option in other scenarios.
        writeOptions.put("storage_client_endpoint_override", "http://localhost:9090");
        // 5MiB for testing. The default is 100MiB. It controls chunk size for multipart upload
        writeOptions.put("storage_client_max_chunk_size_in_bytes", "5242880");
        // 10MiB for testing. The default is 5GiB. It controls object size on S3
        writeOptions.put("max_size_per_sstable_bundle_in_bytes_s3_transport", "10485760");
        writeOptions.put("max_job_duration_minutes", "10");
        writeOptions.put("job_id", "a_unique_id_made_of_arbitrary_string");

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors",
                                            sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;
        Map<String, String> readerOptions = new HashMap<>();
        readerOptions.put("sidecar_instances", "localhost,localhost2,localhost3");
        readerOptions.put("keyspace", "spark_test");
        readerOptions.put("table", "test");
        readerOptions.put("DC", "datacenter1");
        readerOptions.put("snapshotName", UUID.randomUUID().toString());
        readerOptions.put("createSnapshot", "true");
        readerOptions.put("defaultParallelism", String.valueOf(sc.defaultParallelism()));
        readerOptions.put("numCores", String.valueOf(numCores));
        readerOptions.put("sizing", "default");

        JobConfiguration config = new JobConfiguration(writeOptions, readerOptions); // empty read option since the job does not perform read.
        config.rowCount = 2_000_000L;
        return config;
    }
}
