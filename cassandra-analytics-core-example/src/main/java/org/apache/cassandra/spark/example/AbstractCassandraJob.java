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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * Example showcasing the Cassandra Spark Analytics write and read capabilities
 *
 * <p>Prepare your environment by creating the following keyspace and table
 *
 * <p>Schema for the {@code keyspace}:
 * <pre>
 *     CREATE KEYSPACE spark_test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}
 *     AND durable_writes = true;
 * </pre>
 *
 * <p>Schema for the {@code table}:
 * <pre>
 *     CREATE TABLE spark_test.test (
 *     id BIGINT PRIMARY KEY,
 *     course BLOB,
 *     marks BIGINT
 *     );
 * </pre>
 */
public abstract class AbstractCassandraJob
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private JobConfiguration configuration;

    protected abstract JobConfiguration configureJob(SparkContext sc, SparkConf sparkConf);

    public void start(String[] args)
    {
        logger.info("Starting Spark job with args={}", Arrays.toString(args));

        SparkConf sparkConf = new SparkConf().setAppName("Sample Spark Cassandra Bulk Reader Job")
                                             .set("spark.master", "local[8]");

        // Add SBW-specific settings
        // TODO: simplify setting up spark conf
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);

        SparkSession spark = SparkSession
                             .builder()
                             .config(sparkConf)
                             .getOrCreate();
        SparkContext sc = spark.sparkContext();
        SQLContext sql = spark.sqlContext();
        logger.info("Spark Conf: " + sparkConf.toDebugString());

        configuration = configureJob(sc, sparkConf);
        long rowCount = configuration.rowCount;

        try
        {
            Dataset<Row> written = null;
            if (configuration.shouldWrite())
            {
                written = write(rowCount, sql, sc);
            }
            Dataset<Row> read = null;
            if (configuration.shouldRead())
            {
                read = read(rowCount, sql);
            }

            if (configuration.shouldWrite() && configuration.shouldRead())
            {
                checkSmallDataFrameEquality(written, read);
            }
            logger.info("Finished Spark job, shutting down...");
            sc.stop();
        }
        catch (Throwable throwable)
        {
            logger.error("Unexpected exception executing Spark job", throwable);
            try
            {
                sc.stop();
            }
            catch (Throwable ignored)
            {
            }
        }
    }

    protected Dataset<Row> write(long rowCount, SQLContext sql, SparkContext sc)
    {
        StructType schema = new StructType()
                            .add("id", LongType, false)
                            .add("course", BinaryType, false)
                            .add("marks", LongType, false);

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);
        int parallelism = sc.defaultParallelism();
        JavaRDD<Row> rows = genDataset(javaSparkContext, rowCount, parallelism);
        Dataset<Row> df = sql.createDataFrame(rows, schema);

        DataFrameWriter<Row> writer = df.write().format("org.apache.cassandra.spark.sparksql.CassandraDataSink");
        writer.options(configuration.writeOptions);
        writer.mode("append").save();
        return df;
    }

    protected Dataset<Row> read(long expectedRowCount, SQLContext sql)
    {
        DataFrameReader reader = sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource");
        reader.options(configuration.readOptions);
        Dataset<Row> df = reader.load();
        long count = df.count();
        logger.info("Found {} records", count);

        if (count != expectedRowCount)
        {
            logger.error("Expected {} records but found {} records", expectedRowCount, count);
            return null;
        }
        return df;
    }

    private static void checkSmallDataFrameEquality(Dataset<Row> expected, Dataset<Row> actual)
    {
        if (actual == null)
        {
            throw new NullPointerException("actual dataframe is null");
        }
        if (!actual.exceptAll(expected).isEmpty())
        {
            throw new IllegalStateException("The content of the dataframes differs");
        }
    }

    private static JavaRDD<Row> genDataset(JavaSparkContext sc, long records, Integer parallelism)
    {
        long recordsPerPartition = records / parallelism;
        long remainder = records - (recordsPerPartition * parallelism);
        List<Integer> seq = IntStream.range(0, parallelism).boxed().collect(Collectors.toList());
        return sc.parallelize(seq, parallelism).mapPartitionsWithIndex(
        (Function2<Integer, Iterator<Integer>, Iterator<Row>>) (index, integerIterator) -> {
            long firstRecordNumber = index * recordsPerPartition;
            long recordsToGenerate = (index.equals(parallelism)) ? remainder : recordsPerPartition;
            return LongStream.range(0, recordsToGenerate).mapToObj((offset) -> {
                long i = firstRecordNumber + offset;
                String courseNameString = String.valueOf(i);
                int courseNameStringLen = courseNameString.length();
                int courseNameMultiplier = 1000 / courseNameStringLen;
                byte[] courseName = dupStringAsBytes(courseNameString, courseNameMultiplier);
                return RowFactory.create(i, courseName, i);
            }).iterator();
        }, false);
    }

    private static byte[] dupStringAsBytes(String string, Integer times)
    {
        byte[] stringBytes = string.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(stringBytes.length * times);
        for (int i = 0; i < times; i++)
        {
            buf.put(stringBytes);
        }
        return buf.array();
    }

    static class JobConfiguration
    {
        long rowCount = 100_000; // being mutable deliberately for testing convenience.
        ImmutableMap<String, String> writeOptions;
        ImmutableMap<String, String> readOptions;

        JobConfiguration(Map<String, String> writeOptions, Map<String, String> readOptions)
        {
            this.writeOptions = ImmutableMap.copyOf(writeOptions);
            this.readOptions = ImmutableMap.copyOf(readOptions);
        }

        boolean shouldWrite()
        {
            return !writeOptions.isEmpty();
        }

        boolean shouldRead()
        {
            return !readOptions.isEmpty();
        }
    }
}
