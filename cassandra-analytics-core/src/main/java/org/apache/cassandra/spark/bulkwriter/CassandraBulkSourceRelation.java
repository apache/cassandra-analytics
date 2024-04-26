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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobStatus;
import o.a.c.sidecar.client.shaded.common.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.common.stats.JobStatsListener;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportHandler;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
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
    private final JobStatsListener jobStatsListener;
    private final BulkWriteValidator writeValidator;
    private HeartbeatReporter heartbeatReporter;
    private long startTimeNanos;

    @SuppressWarnings("RedundantTypeArguments")
    public CassandraBulkSourceRelation(BulkWriterContext writerContext, SQLContext sqlContext)
    {
        this.writerContext = writerContext;
        this.sqlContext = sqlContext;
        this.sparkContext = JavaSparkContext.fromSparkContext(sqlContext.sparkContext());
        this.broadcastContext = sparkContext.<BulkWriterContext>broadcast(writerContext);
        ReplicaAwareFailureHandler<RingInstance> failureHandler = new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
        this.writeValidator = new BulkWriteValidator(writerContext, failureHandler);
        onCloudStorageTransport(ignored -> this.heartbeatReporter = new HeartbeatReporter());
        this.jobStatsListener = new JobStatsListener((jobEventDetail) -> {
            // Note: Consumers are called for all jobs and tasks. We only publish for the existing job
            if (writerContext.job().getId().equals(jobEventDetail.internalJobID()))
            {
                writerContext.jobStats().publish(jobEventDetail.jobStats());
            }
        });

        this.sparkContext.sc().addSparkListener(jobStatsListener);
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
        validateJob(overwrite);
        this.startTimeNanos = System.nanoTime();
        maybeEnableTransportExtension();
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

    private void validateJob(boolean overwrite)
    {
        if (overwrite)
        {
            throw new LoadNotSupportedException("Overwriting existing data needs TRUNCATE on Cassandra, which is not supported");
        }
        writerContext.cluster().checkBulkWriterIsEnabledOrThrow();
    }

    public void cancelJob(@NotNull CancelJobEvent cancelJobEvent)
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
        try
        {
            onCloudStorageTransport(ctx -> abortRestoreJob(ctx, cancelJobEvent.exception));
        }
        finally
        {
            sparkContext.cancelJobGroup(writerContext.job().getId());
        }
    }

    private void persist(@NotNull JavaPairRDD<DecoratedKey, Object[]> sortedRDD, String[] columnNames)
    {
        onDirectTransport(ctx -> writeValidator.setPhase("UploadAndCommit"));
        onCloudStorageTransport(ctx -> {
            writeValidator.setPhase("UploadToCloudStorage");
            ctx.transportExtensionImplementation().onTransportStart(elapsedTimeMillis());
        });

        try
        {
            // Copy the broadcast context as a local variable (by passing as the input) to avoid serialization error
            // W/o this, SerializedLambda captures the CassandraBulkSourceRelation object, which is not serializable (required by Spark),
            // as a captured argument. It causes "Task not serializable" error.
            List<WriteResult> writeResults = sortedRDD
                                             .mapPartitions(writeRowsInPartition(broadcastContext, columnNames))
                                             .collect();

            // Unpersist broadcast context to free up executors while driver waits for the
            // import to complete
            unpersist();

            List<StreamResult> streamResults = writeResults.stream()
                                                           .map(WriteResult::streamResults)
                                                           .flatMap(Collection::stream)
                                                           .collect(Collectors.toList());

            long rowCount = streamResults.stream().mapToLong(res -> res.rowCount).sum();
            long totalBytesWritten = streamResults.stream().mapToLong(res -> res.bytesWritten).sum();
            boolean hasClusterTopologyChanged = writeResults.stream()
                                                            .anyMatch(WriteResult::isClusterResizeDetected);

            onCloudStorageTransport(context -> {
                LOGGER.info("Waiting for Cassandra to complete import slices. rows={} bytes={} cluster_resized={}",
                            rowCount,
                            totalBytesWritten,
                            hasClusterTopologyChanged);

                // Update with the stream result from tasks.
                // Some token ranges might fail on instances, but the CL is still satisfied at this step
                writeValidator.updateFailureHandler(streamResults);

                List<BlobStreamResult> resultsAsBlobStreamResults = streamResults.stream()
                                                                                 .map(BlobStreamResult.class::cast)
                                                                                 .collect(Collectors.toList());

                int objectsCount = resultsAsBlobStreamResults.stream()
                                                             .mapToInt(res -> res.createdRestoreSlices.size())
                                                             .sum();
                // report the number of objects persisted on s3
                LOGGER.info("Notifying extension all objects have been persisted, totaling {} objects", objectsCount);
                context.transportExtensionImplementation()
                       .onAllObjectsPersisted(objectsCount, rowCount, elapsedTimeMillis());

                ImportCompletionCoordinator.of(startTimeNanos, writerContext, context.dataTransferApi(),
                                               writeValidator, resultsAsBlobStreamResults,
                                               context.transportExtensionImplementation(), this::cancelJob)
                                           .waitForCompletion();
                markRestoreJobAsSucceeded(context);
            });

            LOGGER.info("Bulk writer job complete. rows={} bytes={} cluster_resize={}",
                        rowCount,
                        totalBytesWritten,
                        hasClusterTopologyChanged);
            publishSuccessfulJobStats(rowCount, totalBytesWritten, hasClusterTopologyChanged);
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Bulk Write Failed", throwable);
            RuntimeException failure = new RuntimeException("Bulk Write to Cassandra has failed", throwable);
            try
            {
                onCloudStorageTransport(ctx -> abortRestoreJob(ctx, throwable));
            }
            catch (Exception rte)
            {
                failure.addSuppressed(rte);
            }

            throw failure;
        }
        finally
        {
            try
            {
                onCloudStorageTransport(ignored -> heartbeatReporter.close());
                writerContext.shutdown();
                sqlContext().sparkContext().clearJobGroup();
            }
            catch (Exception ignored)
            {
                LOGGER.warn("Ignored exception during spark job shutdown.", ignored);
                // We've made our best effort to close the Bulk Writer context
            }
            unpersist();
        }
    }

    private void publishSuccessfulJobStats(long rowCount, long totalBytesWritten, boolean hasClusterTopologyChanged)
    {
        Map<String, String> stats = new HashMap<String, String>()
        {
            {
                put("jobId", writerContext.job().getId().toString());
                put("transportInfo", writerContext.job().transportInfo().toString());
                put("rowsWritten", Long.toString(rowCount));
                put("bytesWritten", Long.toString(totalBytesWritten));
                put("clusterResizeDetected", String.valueOf(hasClusterTopologyChanged));
            }
        };
        writerContext.jobStats().publish(stats);
    }

    /**
     * Get a ref copy of BulkWriterContext broadcast variable and compose a function to transform a partition into StreamResult
     *
     * @param ctx BulkWriterContext broadcast variable
     * @return FlatMapFunction
     */
    private static FlatMapFunction<Iterator<Tuple2<DecoratedKey, Object[]>>, WriteResult>
    writeRowsInPartition(Broadcast<BulkWriterContext> ctx, String[] columnNames)
    {
        return iterator -> Collections.singleton(new RecordWriter(ctx.getValue(), columnNames).write(iterator)).iterator();
    }

    /**
     * Deletes cached copies of the broadcast on the executors
     */
    protected void unpersist()
    {
        try
        {
            LOGGER.info("Unpersisting broadcast context");
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

    // initialization for CloudStorageTransport
    private void maybeEnableTransportExtension()
    {
        onCloudStorageTransport(ctx -> {
            StorageTransportHandler storageTransportHandler = new StorageTransportHandler(ctx, writerContext.job(), this::cancelJob);
            StorageTransportExtension impl = ctx.transportExtensionImplementation();
            impl.setCredentialChangeListener(storageTransportHandler);
            impl.setObjectFailureListener(storageTransportHandler);
            createRestoreJob(ctx);
            heartbeatReporter.schedule("Extend lease",
                                       TimeUnit.MINUTES.toMillis(1),
                                       () -> extendLeaseForJob(ctx));
        });
    }

    private void extendLeaseForJob(TransportContext.CloudStorageTransportContext ctx)
    {
        UpdateRestoreJobRequestPayload payload = new UpdateRestoreJobRequestPayload(null, null, null, updatedLeaseTime());
        try
        {
            ctx.dataTransferApi().updateRestoreJob(payload);
        }
        catch (ClientException e)
        {
            LOGGER.warn("Failed to update expireAt for job", e);
        }
    }

    private long updatedLeaseTime()
    {
        return System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(writerContext.job().jobKeepAliveMinutes());
    }

    private long elapsedTimeMillis()
    {
        long now = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(now - this.startTimeNanos);
    }

    void onCloudStorageTransport(Consumer<TransportContext.CloudStorageTransportContext> consumer)
    {
        TransportContext transportContext = writerContext.transportContext();
        if (transportContext instanceof TransportContext.CloudStorageTransportContext)
        {
            consumer.accept((TransportContext.CloudStorageTransportContext) transportContext);
        }
    }

    void onDirectTransport(Consumer<TransportContext.DirectDataBulkWriterContext> consumer)
    {
        TransportContext transportContext = writerContext.transportContext();
        if (transportContext instanceof TransportContext.DirectDataBulkWriterContext)
        {
            consumer.accept((TransportContext.DirectDataBulkWriterContext) transportContext);
        }
    }

    private void createRestoreJob(TransportContext.CloudStorageTransportContext context)
    {
        StorageTransportConfiguration conf = context.transportConfiguration();
        RestoreJobSecrets secrets = conf.getStorageCredentialPair().toRestoreJobSecrets(conf.getReadRegion(),
                                                                                        conf.getWriteRegion());
        JobInfo job = writerContext.job();
        CreateRestoreJobRequestPayload payload = CreateRestoreJobRequestPayload
                                                 .builder(secrets, updatedLeaseTime())
                                                 .jobAgent(BuildInfo.APPLICATION_NAME)
                                                 .jobId(job.getRestoreJobId())
                                                 .updateImportOptions(importOptions -> {
                                                     importOptions.verifySSTables(true) // we disallow the end-user to bypass the non-extended verify anymore
                                                                  .extendedVerify(false); // always turn off
                                                 })
                                                 .build();

        try
        {
            context.dataTransferApi().createRestoreJob(payload);
        }
        catch (ClientException e)
        {
            throw new RuntimeException("Failed to create a new restore job on Sidecar", e);
        }
    }

    private void markRestoreJobAsSucceeded(TransportContext.CloudStorageTransportContext context)
    {
        UpdateRestoreJobRequestPayload requestPayload = new UpdateRestoreJobRequestPayload(null, null, RestoreJobStatus.SUCCEEDED, null);
        UUID jobId = writerContext.job().getRestoreJobId();
        try
        {
            LOGGER.info("Marking the restore job as succeeded. jobId={}", jobId);
            // Prioritize the call to extension, so onJobSucceeded is always invoked.
            context.transportExtensionImplementation().onJobSucceeded(elapsedTimeMillis());
            context.dataTransferApi().updateRestoreJob(requestPayload);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to mark the restore job as succeeded. jobId={}", jobId, e);
            // Do not rethrow - avoid triggering the catch block at the call-site that marks job as failed.
        }
    }

    private void abortRestoreJob(TransportContext.CloudStorageTransportContext context, Throwable cause)
    {
        // Prioritize the call to extension, so onJobFailed is always invoked.
        context.transportExtensionImplementation().onJobFailed(elapsedTimeMillis(), cause);
        UUID jobId = writerContext.job().getRestoreJobId();
        try
        {
            LOGGER.info("Aborting job. jobId={}", jobId);
            context.dataTransferApi().abortRestoreJob();
        }
        catch (ClientException e)
        {
            throw new RuntimeException("Failed to abort the restore job on Sidecar. jobId: " + jobId, e);
        }
    }
}
