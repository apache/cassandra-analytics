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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * Spark job for use in integration tests. It contains a framework for generating data,
 * writing using the bulk writer, and then reading that data back using the reader.
 * It has extension points for the actual row and schema generation.
 */
public final class IntegrationTestJob implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestJob.class);
    @NotNull
    private final RowGenerator rowGenerator;
    // Transient because we only need it on the driver.
    @NotNull
    private final transient Function<Dataset<Row>, Dataset<Row>> postWriteDatasetModifier;
    private final int rowCount;
    private final int sidecarPort;
    private final StructType writeSchema;
    @NotNull
    private final Map<String, String> extraWriterOptions;
    private final boolean shouldWrite;
    private final boolean shouldRead;
    private final String table;
    private String keyspace;

    //CHECKSTYLE IGNORE: This is only called from the Builder
    private IntegrationTestJob(@NotNull RowGenerator rowGenerator,
                               @NotNull StructType writeSchema,
                               @NotNull Function<Dataset<Row>, Dataset<Row>> postWriteDatasetModifier,
                               @NotNull int rowCount,
                               @NotNull int sidecarPort,
                               @NotNull Map<String, String> extraWriterOptions,
                               boolean shouldWrite,
                               boolean shouldRead, String keyspace, String table)
    {
        this.rowGenerator = rowGenerator;
        this.postWriteDatasetModifier = postWriteDatasetModifier;
        this.rowCount = rowCount;
        this.sidecarPort = sidecarPort;
        this.writeSchema = writeSchema;
        this.extraWriterOptions = extraWriterOptions;
        this.shouldWrite = shouldWrite;
        this.shouldRead = shouldRead;
        this.keyspace = keyspace;
        this.table = table;
    }

    public static Builder builder(RowGenerator rowGenerator, StructType writeSchema)
    {
        return new Builder(rowGenerator, writeSchema);
    }

    private static void checkSmallDataFrameEquality(Dataset<Row> expected, Dataset<Row> actual)
    {
        if (actual == null)
        {
            throw new NullPointerException("actual dataframe is null");
        }
        if (expected == null)
        {
            throw new NullPointerException("expected dataframe is null");
        }
        // Simulate `actual` having fewer rows, but all match rows in `expected`.
        // The previous implementation would consider these equal
        // actual = actual.limit(1000);
        if (!actual.exceptAll(expected).isEmpty() || !expected.exceptAll(actual).isEmpty())
        {
            throw new IllegalStateException("The content of the dataframes differs");
        }
    }

    private Dataset<Row> write(SQLContext sql, SparkContext sc)
    {
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<Row> rows = genDataset(javaSparkContext, sc.defaultParallelism());
        Dataset<Row> df = sql.createDataFrame(rows, writeSchema);

        DataFrameWriter<Row> dfWriter = df.write()
                                          .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                          .option("sidecar_instances", "localhost,localhost2,localhost3")
                                          .option("sidecar_port", sidecarPort)
                                          .option("keyspace", keyspace)
                                          .option("table", table)
                                          .option("local_dc", "datacenter1")
                                          .option("bulk_writer_cl", "LOCAL_QUORUM")
                                          .option("number_splits", "-1")
                                          .mode("append");
        dfWriter.options(extraWriterOptions);
        dfWriter.save();
        return df;
    }

    private Dataset<Row> read(SparkConf sparkConf, SQLContext sql, SparkContext sc)
    {
        int expectedRowCount = rowCount;
        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors", sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;

        Dataset<Row> df = sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                             .option("sidecar_instances", "localhost,localhost2,localhost3")
                             .option("sidecar_port", sidecarPort)
                             .option("keyspace", "spark_test")
                             .option("table", "test")
                             .option("DC", "datacenter1")
                             .option("snapshotName", UUID.randomUUID().toString())
                             .option("createSnapshot", "true")
                             // Shutdown hooks are called after the job ends, and in the case of integration tests
                             // the sidecar is already shut down before this. Since the cluster will be torn
                             // down anyway, the integration job skips clearing snapshots.
                             .option("clearSnapshot", "false")
                             .option("defaultParallelism", sc.defaultParallelism())
                             .option("numCores", numCores)
                             .option("sizing", "default")
                             .load();

        long count = df.count();
        LOGGER.info("Found {} records", count);

        if (count != expectedRowCount)
        {
            throw new RuntimeException(String.format("Expected %d records but found %d records",
                                                     expectedRowCount,
                                                     count));
        }
        return df;
    }

    private JavaRDD<Row> genDataset(JavaSparkContext sc, int parallelism)
    {
        long recordsPerPartition = rowCount / parallelism;
        long remainder = rowCount - (recordsPerPartition * parallelism);
        List<Integer> seq = IntStream.range(0, parallelism).boxed().collect(Collectors.toList());
        JavaRDD<Row> dataset = sc.parallelize(seq, parallelism).mapPartitionsWithIndex(
        (Function2<Integer, Iterator<Integer>, Iterator<Row>>) (index, integerIterator) -> {
            long firstRecordNumber = index * recordsPerPartition;
            long recordsToGenerate = index.equals(parallelism) ? remainder : recordsPerPartition;
            java.util.Iterator<Row> rows = LongStream.range(0, recordsToGenerate).mapToObj(offset -> {
                long recordNumber = firstRecordNumber + offset;
                return rowGenerator.rowFor(recordNumber);
            }).iterator();
            return rows;
        }, false);
        return dataset;
    }

    public void run()
    {
        LOGGER.info("Starting Spark job with args={}", this);

        SparkConf sparkConf = new SparkConf().setAppName("Sample Spark Cassandra Bulk Reader Job")
                                             .set("spark.master", "local[8]");

        // Add SBW-specific settings
        // TODO: Simplify setting up spark conf
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);

        SparkSession spark = SparkSession
                             .builder()
                             .config(sparkConf)
                             .getOrCreate();
        SparkContext sc = spark.sparkContext();
        SQLContext sql = spark.sqlContext();
        LOGGER.info("Spark Conf: " + sparkConf.toDebugString());

        try
        {
            Dataset<Row> written = null;
            Dataset<Row> read = null;
            if (shouldWrite)
            {
                written = write(sql, sc);
                written = postWriteDatasetModifier.apply(written);
            }

            if (shouldRead)
            {
                read = read(sparkConf, sql, sc);
            }
            if (read != null && written != null)
            {
                checkSmallDataFrameEquality(written, read);
            }
            LOGGER.info("Finished Spark job, shutting down...");
            sc.stop();
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Unexpected exception executing Spark job", throwable);
            try
            {
                sc.stop();
            }
            catch (Throwable ignored)
            {
            }
            throw throwable; // rethrow so the exception bubbles up to test usages.
        }
    }

    @Override
    public String toString()
    {
        return "IntegrationTestJob: { \n " +
               "    rowCount:%d,\n" +
               "    parallelism:%d,\n" +
               "    ttl:%d,\n" +
               "    timestamp:%d,\n" +
               "    sidecarPort:%d\n" +
               "}";
    }

    @FunctionalInterface
    public interface RowGenerator extends Serializable
    {
        Row rowFor(long recordNumber);
    }

    public static class Builder
    {
        private final RowGenerator rowGenerator;
        private final StructType writeSchema;
        private int rowCount = 10_000;
        private int sidecarPort = 9043;
        private Map<String, String> extraWriterOptions;
        private Function<Dataset<Row>, Dataset<Row>> postWriteDatasetModifier = Function.identity();
        private boolean shouldWrite = true;
        private boolean shouldRead = true;
        private String keyspace = "spark_test";
        private String table = "test";

        Builder(@NotNull RowGenerator rowGenerator, StructType writeSchema)
        {
            this.rowGenerator = rowGenerator;
            this.writeSchema = writeSchema;
        }

        public Builder withKeyspace(String keyspace)
        {
            return update(builder -> builder.keyspace = keyspace);
        }

        public Builder withTable(String table)
        {
            return update(builder -> builder.table = table);
        }

        public Builder withRowCount(int rowCount)
        {
            return update(builder -> builder.rowCount = rowCount);
        }

        public Builder withSidecarPort(int sidecarPort)
        {
            return update(builder -> builder.sidecarPort = sidecarPort);
        }

        private Builder update(Consumer<Builder> update)
        {
            update.accept(this);
            return this;
        }

        public Builder withExtraWriterOptions(Map<String, String> writerOptions)
        {
            return update(builder -> builder.extraWriterOptions = writerOptions);
        }

        public Builder withPostWriteDatasetModifier(Function<Dataset<Row>, Dataset<Row>> dataSetModifier)
        {
            return update(builder -> builder.postWriteDatasetModifier = dataSetModifier);
        }


        public Builder shouldWrite(boolean shouldWrite)
        {
            return update(builder -> builder.shouldWrite = shouldWrite);
        }

        public Builder shouldRead(boolean shouldRead)
        {
            return update(builder -> builder.shouldRead = shouldRead);
        }


        public IntegrationTestJob build()
        {
            return new IntegrationTestJob(this.rowGenerator,
                                          this.writeSchema,
                                          this.postWriteDatasetModifier,
                                          this.rowCount,
                                          this.sidecarPort,
                                          this.extraWriterOptions,
                                          this.shouldWrite,
                                          this.shouldRead,
                                          this.keyspace,
                                          this.table);
        }

        public void run()
        {
            build().run();
        }
    }
}
