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

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.util.control.NonFatal$;

public class CassandraBulkSourceRelation extends BaseRelation implements InsertableRelation
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBulkSourceRelation.class);

    private final BulkWriterContext writerContext;
    private final SQLContext sqlContext;
    private final JavaSparkContext sparkContext;
    private final Broadcast<BulkWriterContext> broadcastContext;
    private final BulkWriteValidator writeValidator;

    @SuppressWarnings("RedundantTypeArguments")
    public CassandraBulkSourceRelation(BulkWriterContext writerContext, SQLContext sqlContext) throws Exception
    {
        this.writerContext = writerContext;
        this.sqlContext = sqlContext;
        this.sparkContext = JavaSparkContext.fromSparkContext(sqlContext.sparkContext());
        this.broadcastContext = sparkContext.<BulkWriterContext>broadcast(writerContext);
        this.writeValidator = new BulkWriteValidator(writerContext, this::cancelJob);
    }

    @Override
    @NotNull
    public SQLContext sqlContext()
    {
        return sqlContext;
    }

    /**
     * @return An empty {@link StructType}, as this is a writer only, so schema is not applicable
     */
    @Override
    @NotNull
    public StructType schema()
    {
        LOGGER.warn("This instance is used as writer, a schema is not supported");
        return new StructType();
    }

    /**
     * @return {@code 0} size as not applicable use by the planner in the writer-only use case
     */
    @Override
    public long sizeInBytes()
    {
        LOGGER.warn("This instance is used as writer, sizeInBytes is not supported");
        return 0L;
    }

    @Override
    public void insert(@NotNull Dataset<Row> data, boolean overwrite)
    {
        if (overwrite)
        {
            throw new LoadNotSupportedException("Overwriting existing data needs TRUNCATE on Cassandra, which is not supported");
        }
        writerContext.cluster().checkBulkWriterIsEnabledOrThrow();
        Tokenizer tokenizer = new Tokenizer(writerContext);
        TableSchema tableSchema = writerContext.schema().getTableSchema();
        JavaPairRDD<DecoratedKey, Object[]> sortedRDD = data.toJavaRDD()
                .map(Row::toSeq)
                .map(seq -> JavaConverters.seqAsJavaListConverter(seq).asJava().toArray())
                .map(tableSchema::normalize)
                .keyBy(tokenizer::getDecoratedKey)
                .repartitionAndSortWithinPartitions(broadcastContext.getValue().job().getTokenPartitioner());
        persist(sortedRDD, data.columns());
    }

    private void cancelJob(@NotNull CancelJobEvent cancelJobEvent)
    {
        if (cancelJobEvent.exception != null)
        {
            LOGGER.error("An unrecoverable error occurred during {} stage of import while validating the current cluster state; cancelling job",
                         writeValidator.getPhase(), cancelJobEvent.exception);
        }
        else
        {
            LOGGER.error("Job was canceled due to '{}' during {} stage of import; please rerun import once topology changes are complete",
                         cancelJobEvent.reason, writeValidator.getPhase());
        }
        sparkContext.cancelJobGroup(writerContext.job().getId().toString());
    }

    @SuppressWarnings("RedundantCast")
    private void persist(@NotNull JavaPairRDD<DecoratedKey, Object[]> sortedRDD, String[] columnNames)
    {
        writeValidator.setPhase("Environment Validation");
        writeValidator.validateInitialEnvironment();
        writeValidator.setPhase("UploadAndCommit");

        try
        {
            sortedRDD.foreachPartition(writeRowsInPartition(broadcastContext, columnNames));
            writeValidator.failIfRingChanged();
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Bulk Write Failed", throwable);
            throw new RuntimeException("Bulk Write to Cassandra has failed", throwable);
        }
        finally
        {
            writeValidator.close();  // Uses the MgrClient, so needs to stop first
            try
            {
                writerContext.shutdown();
                sqlContext().sparkContext().clearJobGroup();
            }
            catch (Exception ignored)
            {
                // We've made our best effort to close the Bulk Writer context
            }
            try
            {
                broadcastContext.unpersist(false);
            }
            catch (Throwable throwable)
            {
                if (NonFatal$.MODULE$.apply(throwable))
                {
                    LOGGER.error("Uncaught exception in thread {} attempting to unpersist broadcast variable",
                                 Thread.currentThread().getName(), throwable);
                }
                else
                {
                    throw throwable;
                }
            }
        }
    }

    private static VoidFunction<Iterator<Tuple2<DecoratedKey, Object[]>>> writeRowsInPartition(Broadcast<BulkWriterContext> broadcastContext,
                                                                                               String[] columnNames)
    {
        return itr -> new RecordWriter(broadcastContext.getValue(), columnNames).write(itr);
    }
}
