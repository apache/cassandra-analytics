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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobStatus;
import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageStreamResult;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.ImportCoordinator;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.ImportCompletionCoordinator;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedImportCoordinator;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.UnsupportedAnalyticsOperationException;
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
    private final SimpleTaskScheduler simpleTaskScheduler;
    private ImportCoordinator importCoordinator = null; // value is only set when using S3_COMPAT
    private long startTimeNanos;

    @SuppressWarnings("RedundantTypeArguments")
    public CassandraBulkSourceRelation(BulkWriterContext writerContext, SQLContext sqlContext)
    {
        this.writerContext = writerContext;
        this.sqlContext = sqlContext;
        this.sparkContext = JavaSparkContext.fromSparkContext(sqlContext.sparkContext());
        this.broadcastContext = sparkContext.<BulkWriterContext>broadcast(writerContext);
        ReplicaAwareFailureHandler<RingInstance> failureHandler = new MultiClusterReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
        this.writeValidator = new BulkWriteValidator(writerContext, failureHandler);
        this.simpleTaskScheduler = new SimpleTaskScheduler();
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
        maybeScheduleTimeout();
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
            throw new UnsupportedAnalyticsOperationException("Overwriting existing data needs TRUNCATE on Cassandra, which is not supported");
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
            boolean hasClusterTopologyChanged = writeResults.stream().anyMatch(WriteResult::isClusterResizeDetected);

            onCloudStorageTransport(context -> waitForImportCompletion(context, rowCount, totalBytesWritten, hasClusterTopologyChanged, streamResults));

            LOGGER.info("Bulk writer job complete. rowCount={} totalBytes={} hasClusterTopologyChanged={}",
                        rowCount, totalBytesWritten, hasClusterTopologyChanged);
            publishSuccessfulJobStats(rowCount, totalBytesWritten, hasClusterTopologyChanged);
        }
        catch (Throwable throwable)
        {
            publishFailureJobStats(throwable.getMessage());
            LOGGER.error("Bulk Write Failed.", throwable);
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
                simpleTaskScheduler.close();
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

    private void waitForImportCompletion(TransportContext.CloudStorageTransportContext context,
                                         long rowCount,
                                         long totalBytesWritten,
                                         boolean hasClusterTopologyChanged,
                                         List<StreamResult> streamResults)
    {
        LOGGER.info("Waiting for Cassandra to complete import slices. rowCount={} totalBytes={} hasClusterTopologyChanged={}",
                    rowCount,
                    totalBytesWritten,
                    hasClusterTopologyChanged);

        List<StreamError> allErrors = streamResults.stream().flatMap(r -> r.failures.stream()).collect(Collectors.toList());
        if (writerContext.job().isCoordinatedWriteEnabled())
        {
            if (!allErrors.isEmpty())
            {
                throw new IllegalStateException("Stream errors are unexpected when coordinated-write is enabled. streamErrors=" + allErrors);
            }
        }
        else
        {
            // Update with the stream result from tasks.
            // Some token ranges might fail on instances, but the CL is still satisfied at this step
            writeValidator.updateFailureHandler(allErrors);
        }

        List<CloudStorageStreamResult> resultsAsCloudStorageStreamResults = streamResults.stream()
                                                                                         .map(CloudStorageStreamResult.class::cast)
                                                                                         .collect(Collectors.toList());

        int objectCount = resultsAsCloudStorageStreamResults.stream()
                                                            .mapToInt(res -> res.objectCount)
                                                            .sum();
        long elapsedInMillis = elapsedTimeMillis();
        LOGGER.info("Notifying extension all objects and rows have been persisted. objectCount={} rowCount={} timeElapsedInMillis={}",
                    objectCount, rowCount, elapsedInMillis);
        context.transportExtensionImplementation()
               .onAllObjectsPersisted(objectCount, rowCount, elapsedTimeMillis());

        setSliceCountForRestoreJob(context, objectCount);

        awaitImportCompletion(context, resultsAsCloudStorageStreamResults);
        markRestoreJobAsSucceeded(context);
    }

    private void awaitImportCompletion(TransportContext.CloudStorageTransportContext context,
                                       List<CloudStorageStreamResult> resultsAsCloudStorageStreamResults)
    {
        // create for non-coordinated-write mode.
        // for coordinated write, the import coordinator is created when starting the job
        if (!writerContext.job().isCoordinatedWriteEnabled())
        {
            importCoordinator = ImportCompletionCoordinator.of(startTimeNanos, writerContext, context.dataTransferApi(),
                                                               writeValidator, resultsAsCloudStorageStreamResults,
                                                               context.transportExtensionImplementation(), this::cancelJob);
        }
        Objects.requireNonNull(importCoordinator, "importCoordinator is not initialized");
        importCoordinator.await();
    }

    private void publishSuccessfulJobStats(long rowCount, long totalBytesWritten, boolean hasClusterTopologyChanged)
    {
        writerContext.jobStats().publish(new HashMap<String, String>() // type declaration required to compile with java8
        {{
                put("jobId", writerContext.job().getId().toString());
                put("transportInfo", writerContext.job().transportInfo().toString());
                put("rowsWritten", Long.toString(rowCount));
                put("bytesWritten", Long.toString(totalBytesWritten));
                put("jobStatus", "Succeeded");
                put("clusterResizeDetected", String.valueOf(hasClusterTopologyChanged));
                put("jobElapsedTimeMillis", Long.toString(elapsedTimeMillis()));
        }});
    }

    private void publishFailureJobStats(String reason)
    {
        writerContext.jobStats().publish(new HashMap<String, String>() // type declaration required to compile with java8
        {{
                put("jobId", writerContext.job().getId().toString());
                put("transportInfo", writerContext.job().transportInfo().toString());
                put("jobStatus", "Failed");
                put("failureReason", reason);
                put("jobElapsedTimeMillis", Long.toString(elapsedTimeMillis()));
        }});
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
            JobInfo job = writerContext.job();
            StorageTransportHandler storageTransportHandler = new StorageTransportHandler(ctx, job, this::cancelJob);
            StorageTransportExtension impl = ctx.transportExtensionImplementation();
            impl.setCredentialChangeListener(storageTransportHandler);
            impl.setObjectFailureListener(storageTransportHandler);
            if (job.isCoordinatedWriteEnabled())
            {
                CloudStorageDataTransferApi dataTransferApi = ctx.dataTransferApi();
                Preconditions.checkState(dataTransferApi instanceof CoordinatedCloudStorageDataTransferApi,
                                         "CoordinatedCloudStorageDataTransferApi is required for coordinated write");
                CoordinatedCloudStorageDataTransferApi api = (CoordinatedCloudStorageDataTransferApi) dataTransferApi;
                CoordinatedImportCoordinator coordinator = CoordinatedImportCoordinator.of(startTimeNanos, job, api, impl);
                this.importCoordinator = coordinator;
                impl.setCoordinationSignalListener(coordinator);
                createRestoreJobsOnAllClusters(ctx, job.coordinatedWriteConf(), api);
            }
            else
            {
                createRestoreJob(ctx);
            }
            simpleTaskScheduler.schedulePeriodic("Extend lease",
                                                 Duration.ofMinutes(1),
                                                 () -> extendLeaseForJob(ctx));
        });
    }

    private void extendLeaseForJob(TransportContext.CloudStorageTransportContext ctx)
    {
        UpdateRestoreJobRequestPayload payload = UpdateRestoreJobRequestPayload.builder().withExpireAtInMillis(updatedLeaseTime()).build();
        try
        {
            ctx.dataTransferApi().updateRestoreJob(payload);
        }
        catch (SidecarApiCallException e)
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
        // todo: refactor to move away from using 'null'
        RestoreJobSecrets secrets = conf.getStorageCredentialPair(null)
                                        .toRestoreJobSecrets();
        JobInfo job = writerContext.job();
        CreateRestoreJobRequestPayload payload = createJobPayloadBuilder(job, secrets).build();
        context.dataTransferApi().createRestoreJob(payload);
    }

    private void createRestoreJobsOnAllClusters(TransportContext.CloudStorageTransportContext context,
                                                CoordinatedWriteConf coordinatedWriteConf,
                                                CoordinatedCloudStorageDataTransferApi dataTransferApi)
    {
        StorageTransportConfiguration conf = context.transportConfiguration();

        JobInfo job = writerContext.job();
        ConsistencyLevel cl = job.getConsistencyLevel();

        // create restore job on each cluster
        dataTransferApi.forEach((clusterId, api) -> {
            RestoreJobSecrets secrets = conf.getStorageCredentialPair(clusterId)
                                            .toRestoreJobSecrets();
            CoordinatedWriteConf.ClusterConf cluster = coordinatedWriteConf.cluster(clusterId);
            String localDc = cluster.resolveLocalDc(cl); // resolve the cluster specific localDc name
            CreateRestoreJobRequestPayload payload = createJobPayloadBuilder(job, secrets)
                                                     .consistencyLevel(toSidecarConsistencyLevel(cl), localDc)
                                                     .build();
            api.createRestoreJob(payload);
        });
    }

    private CreateRestoreJobRequestPayload.Builder createJobPayloadBuilder(JobInfo job, RestoreJobSecrets secrets)
    {
        CreateRestoreJobRequestPayload.Builder builder = CreateRestoreJobRequestPayload.builder(secrets, updatedLeaseTime());
        builder.jobAgent(BuildInfo.APPLICATION_NAME)
               .jobId(job.getRestoreJobId())
               .updateImportOptions(importOptions -> {
                   importOptions.verifySSTables(true) // we disallow the end-user to bypass the non-extended verify anymore
                                .extendedVerify(false); // always turn off
               });
        return builder;
    }

    private o.a.c.sidecar.client.shaded.common.data.ConsistencyLevel toSidecarConsistencyLevel(ConsistencyLevel cl)
    {
        return o.a.c.sidecar.client.shaded.common.data.ConsistencyLevel.fromString(cl.toString());
    }

    private void setSliceCountForRestoreJob(TransportContext.CloudStorageTransportContext context, long sliceCount)
    {
        UpdateRestoreJobRequestPayload requestPayload = UpdateRestoreJobRequestPayload.builder().withSliceCount(sliceCount).build();
        UUID jobId = writerContext.job().getRestoreJobId();
        try
        {
            LOGGER.info("Setting slice count for the restore job. jobId={} sliceCount={}", jobId, sliceCount);
            context.dataTransferApi().updateRestoreJob(requestPayload);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to set slice count for the restore job. jobId={}", jobId, e);
            // Do not rethrow - avoid triggering the catch block at the call-site that marks job as failed.
        }
    }

    private void markRestoreJobAsSucceeded(TransportContext.CloudStorageTransportContext context)
    {
        UpdateRestoreJobRequestPayload requestPayload = UpdateRestoreJobRequestPayload.builder().withStatus(RestoreJobStatus.SUCCEEDED).build();
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
        LOGGER.info("Aborting job. jobId={}", jobId);
        context.dataTransferApi().abortRestoreJob();
    }

    private void maybeScheduleTimeout()
    {
        long timeoutSeconds = writerContext.job().jobTimeoutSeconds();
        if (timeoutSeconds != -1)
        {
            LOGGER.info("Scheduled job timeout. timeoutSeconds={}", timeoutSeconds);
            simpleTaskScheduler.schedule("Job timeout", Duration.ofSeconds(timeoutSeconds), () -> {
                // only cancel on timeout when has not succeeded (consistency level not reached)
                if (importCoordinator == null || !importCoordinator.succeeded())
                {
                    cancelJob(new CancelJobEvent("Job times out after " + timeoutSeconds + " seconds"));
                }
            });
        }
    }
}
