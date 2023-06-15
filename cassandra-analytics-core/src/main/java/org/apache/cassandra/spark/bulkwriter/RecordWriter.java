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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
import scala.Tuple2;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.asScalaIterator;

@SuppressWarnings({"ConstantConditions"})
public class RecordWriter implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);

    private final BulkWriterContext writerContext;
    private final String[] columnNames;
    private Supplier<TaskContext> taskContextSupplier;
    private final BiFunction<BulkWriterContext, Path, SSTableWriter> tableWriterSupplier;
    private SSTableWriter sstableWriter = null;
    private int batchNumber = 0;
    private int batchSize = 0;

    public RecordWriter(BulkWriterContext writerContext, String[] columnNames)
    {
        this(writerContext, columnNames, TaskContext::get, SSTableWriter::new);
    }

    @VisibleForTesting
    RecordWriter(BulkWriterContext writerContext,
                 String[] columnNames,
                 Supplier<TaskContext> taskContextSupplier,
                 BiFunction<BulkWriterContext, Path, SSTableWriter> tableWriterSupplier)
    {
        this.writerContext = writerContext;
        this.columnNames = columnNames;
        this.taskContextSupplier = taskContextSupplier;
        this.tableWriterSupplier = tableWriterSupplier;
    }

    private Range<BigInteger> getTokenRange(TaskContext taskContext)
    {
        return writerContext.job().getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    private String getStreamId(TaskContext taskContext)
    {
        return String.format("%d-%s", taskContext.partitionId(), UUID.randomUUID());
    }

    public StreamResult write(Iterator<Tuple2<DecoratedKey, Object[]>> sourceIterator)
    {
        TaskContext taskContext = taskContextSupplier.get();
        LOGGER.info("[{}]: Processing Bulk Writer partition", taskContext.partitionId());
        scala.collection.Iterator<scala.Tuple2<DecoratedKey, Object[]>> dataIterator =
                new InterruptibleIterator<>(taskContext, asScalaIterator(sourceIterator));
        StreamSession streamSession = createStreamSession(taskContext);
        validateAcceptableTimeSkewOrThrow(streamSession.replicas);
        int partitionId = taskContext.partitionId();
        Range<BigInteger> range = getTokenRange(taskContext);
        JobInfo job = writerContext.job();
        Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"),
                                 job.getId().toString(),
                                 Integer.toString(taskContext.stageAttemptNumber()),
                                 Integer.toString(taskContext.attemptNumber()),
                                 Integer.toString(partitionId));
        Map<String, Object> valueMap = new HashMap<>();
        try
        {
            while (dataIterator.hasNext())
            {
                maybeCreateTableWriter(partitionId, baseDir);
                writeRow(valueMap, dataIterator, partitionId, range);
                checkBatchSize(streamSession, partitionId, job);
            }

            if (batchSize != 0)
            {
               finalizeSSTable(streamSession, partitionId, sstableWriter, batchNumber, batchSize);
            }

            LOGGER.info("[{}] Done with all writers and waiting for stream to complete", partitionId);
            return streamSession.close();
        }
        catch (Exception exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private void validateAcceptableTimeSkewOrThrow(List<RingInstance> replicas)
    {
        TimeSkewResponse timeSkewResponse = writerContext.cluster().getTimeSkew(replicas);
        Instant localNow = Instant.now();
        Instant remoteNow = Instant.ofEpochMilli(timeSkewResponse.currentTime);
        Duration range = Duration.ofMinutes(timeSkewResponse.allowableSkewInMinutes);
        if (localNow.isBefore(remoteNow.minus(range)) || localNow.isAfter(remoteNow.plus(range)))
        {
            String message = String.format("Time skew between Spark and Cassandra is too large. "
                                         + "Allowable skew is %d minutes. "
                                         + "Spark executor time is %s, Cassandra instance time is %s",
                                           timeSkewResponse.allowableSkewInMinutes, localNow, remoteNow);
            throw new UnsupportedOperationException(message);
        }
    }

    public void writeRow(Map<String, Object> valueMap,
                         scala.collection.Iterator<Tuple2<DecoratedKey, Object[]>> dataIterator,
                         int partitionId,
                         Range<BigInteger> range) throws IOException
    {
        Tuple2<DecoratedKey, Object[]> tuple = dataIterator.next();
        DecoratedKey key = tuple._1();
        BigInteger token = key.getToken();
        Preconditions.checkState(range.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, range));
        try
        {
            sstableWriter.addRow(token, getBindValuesForColumns(valueMap, columnNames, tuple._2()));
        }
        catch (RuntimeException exception)
        {
            String message = String.format("[%s]: Failed to write data to SSTable: SBW DecoratedKey was %s",
                                           partitionId, key);
            LOGGER.error(message, exception);
            throw exception;
        }
    }

    public void checkBatchSize(StreamSession streamSession, int partitionId, JobInfo job) throws IOException
    {
        batchSize++;
        if (batchSize > job.getSstableBatchSize())
        {
            finalizeSSTable(streamSession, partitionId, sstableWriter, batchNumber, batchSize);

            sstableWriter = null;
            batchSize = 0;

        }
    }

    public void maybeCreateTableWriter(int partitionId, Path baseDir) throws IOException
    {
        if (sstableWriter == null)
        {
            Path outDir = Paths.get(baseDir.toString(), Integer.toString(++batchNumber));
            Files.createDirectories(outDir);

            sstableWriter = tableWriterSupplier.apply(writerContext, outDir);

            LOGGER.info("[{}][{}] Created new SSTable writer", partitionId, batchNumber);
        }
    }

    private static Map<String, Object> getBindValuesForColumns(Map<String, Object> map, String[] columnNames, Object[] values)
    {
        assert values.length == columnNames.length : "Number of values does not match the number of columns " + values.length + ", " + columnNames.length;
        for (int i = 0; i < columnNames.length; i++)
        {
            map.put(columnNames[i], values[i]);
        }
        return map;
    }

    private void finalizeSSTable(StreamSession streamSession,
                                 int partitionId,
                                 SSTableWriter sstableWriter,
                                 int batchNumber,
                                 int batchSize) throws IOException
    {
        LOGGER.info("[{}][{}] Closing writer and scheduling SStable stream with {} rows",
                    partitionId, batchNumber, batchSize);
        sstableWriter.close(writerContext, partitionId);
        streamSession.scheduleStream(sstableWriter);
    }

    private StreamSession createStreamSession(TaskContext taskContext)
    {
        return new StreamSession(writerContext, getStreamId(taskContext), getTokenRange(taskContext));
    }
}
