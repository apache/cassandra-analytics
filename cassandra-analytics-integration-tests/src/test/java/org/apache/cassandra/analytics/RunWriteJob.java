/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Wrapper to run the spark job in isolation from the main test class
 */
public class RunWriteJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RunWriteJob.class);
    public static void main(String[] args)
    {
        List<String> instances = Arrays.asList(args[0].split(","));
        String keyspace = args[1];
        String table = args[2];
        String consistencyLevel = args[3];
        int sidecarPort = Integer.parseInt(args[4]);
        int rowCount = Integer.parseInt(args[5]);

        new RunWriteJob().runWriteJob(instances, keyspace, table, consistencyLevel, sidecarPort, rowCount);
    }

    void runWriteJob(List<String> sidecarInstances, String keyspace, String table, String consistencyLevel, int sidecarPort, int rowCount)
    {
        SparkConf sparkConf = generateSparkConf();
        try (SparkSession spark = generateSparkSession(sparkConf))
        {
            Dataset<Row> df = generateData(spark, rowCount);

            DataFrameWriter<Row> dfWriter = df.write()
                                              .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                              .option("bulk_writer_cl", consistencyLevel)
                                              .option("local_dc", "datacenter1")
                                              .option("sidecar_instances", String.join(",", sidecarInstances))
                                              .option("sidecar_port", String.valueOf(sidecarPort))
                                              .option("keyspace", keyspace)
                                              .option("table", table)
                                              .option("number_splits", "-1")
                                              .mode("append");

            dfWriter.save();
            spark.sparkContext().stop();
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Failed to run spark job", throwable);
            throw throwable;
        }
    }

    static SparkConf generateSparkConf()
    {
        SparkConf sparkConf = new SparkConf()
                              .setAppName("Integration test Spark Cassandra Bulk Reader Job")
                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                              .set("spark.master", "local[8,4]")
                              .set("spark.cassandra_analytics.sidecar.request.retries", "5")
                              .set("spark.cassandra_analytics.sidecar.request.retries.delay.milliseconds", "1000")
                              .set("spark.cassandra_analytics.sidecar.request.retries.max.delay.milliseconds", "1000");
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);
        return sparkConf;
    }

    static SparkSession generateSparkSession(SparkConf sparkConf)
    {
        return SparkSession.builder()
                           .config(sparkConf)
                           .getOrCreate();
    }

    static Dataset<org.apache.spark.sql.Row> generateData(SparkSession spark, int rowCount)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);

        List<org.apache.spark.sql.Row> rows = IntStream.range(0, rowCount)
                                                       .mapToObj(recordNum -> {
                                                           String course = "course" + recordNum;
                                                           ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNum, course, recordNum));
                                                           return RowFactory.create(values.toArray());
                                                       }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }
}
