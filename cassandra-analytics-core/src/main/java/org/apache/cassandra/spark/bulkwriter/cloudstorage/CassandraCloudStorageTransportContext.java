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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.SortedSSTableWriter;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.jetbrains.annotations.NotNull;

public class CassandraCloudStorageTransportContext implements TransportContext.CloudStorageTransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCloudStorageTransportContext.class);

    @NotNull
    private final StorageTransportExtension storageTransportExtension;
    @NotNull
    private final StorageTransportConfiguration storageTransportConfiguration;
    @NotNull
    private final CloudStorageDataTransferApi dataTransferApi;
    @NotNull
    private final BulkSparkConf conf;
    @NotNull
    private final JobInfo jobInfo;
    @NotNull
    private final ClusterInfo clusterInfo;
    @NotNull
    private final StorageClient storageClient;

    public CassandraCloudStorageTransportContext(@NotNull BulkWriterContext bulkWriterContext,
                                                 @NotNull BulkSparkConf conf,
                                                 boolean isOnDriver)
    {
        // we may not always need a transport extension implementation in cloud based transport context, revisit this
        // check when we have multiple cloud based transport options supported
        Objects.requireNonNull(conf.getTransportInfo().getTransportExtensionClass(),
                               "DATA_TRANSPORT_EXTENSION_CLASS must be provided");
        this.conf = conf;
        this.jobInfo = bulkWriterContext.job();
        this.clusterInfo = bulkWriterContext.cluster();
        this.storageTransportExtension = createStorageTransportExtension(isOnDriver);
        this.storageTransportConfiguration = storageTransportExtension.getStorageConfiguration();
        Objects.requireNonNull(storageTransportConfiguration,
                               "Storage configuration cannot be null in order to upload to cloud");
        this.storageClient = new StorageClient(storageTransportConfiguration, conf.getStorageClientConfig());
        this.dataTransferApi = createDataTransferApi(storageClient);
        Preconditions.checkState(!jobInfo.isCoordinatedWriteEnabled()
                                 || dataTransferApi instanceof CoordinatedCloudStorageDataTransferApi,
                                 "CoordinatedCloudStorageDataTransferApi must be created when coordinated write is enabled");
    }

    @Override
    public CloudStorageStreamSession createStreamSession(BulkWriterContext writerContext,
                                                         String sessionId,
                                                         SortedSSTableWriter sstableWriter,
                                                         Range<BigInteger> range,
                                                         ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                                         ExecutorService executorService)
    {
        return new CloudStorageStreamSession(writerContext, sstableWriter,
                                             this, sessionId, range, failureHandler,
                                             executorService);
    }

    @Override
    public CloudStorageDataTransferApi dataTransferApi()
    {
        return dataTransferApi;
    }

    @NotNull
    @Override
    public StorageTransportConfiguration transportConfiguration()
    {
        return storageTransportConfiguration;
    }

    /**
     * Instantiate and initialize the StorageTransportExtension instance, for only once.
     *
     * @return StorageTransportExtension instance
     */
    @NotNull
    @Override
    public StorageTransportExtension transportExtensionImplementation()
    {
        return this.storageTransportExtension;
    }

    // only invoke it in constructor
    protected CloudStorageDataTransferApi createDataTransferApi(StorageClient storageClient)
    {
        return CloudStorageDataTransferApiFactory.INSTANCE
               .createDataTransferApi(storageClient, jobInfo, clusterInfo);
    }

    // only invoke it in constructor
    protected StorageTransportExtension createStorageTransportExtension(boolean isOnDriver)
    {
        String transportExtensionClass = conf.getTransportInfo().getTransportExtensionClass();
        try
        {
            Class<StorageTransportExtension> clazz = (Class<StorageTransportExtension>) Class.forName(transportExtensionClass);
            StorageTransportExtension extension = clazz.getConstructor().newInstance();
            LOGGER.info("Initializing storage transport extension. jobId={}, restoreJobId={}",
                        jobInfo.getId(), jobInfo.getRestoreJobId());
            extension.initialize(jobInfo.getId(), conf.getSparkConf(), isOnDriver);
            // Only assign when initialization is complete
            // to avoid exposing uninitialized extension object, which leads to unexpected behavior
            return extension;
        }
        catch (ClassNotFoundException | ClassCastException | InvocationTargetException | InstantiationException
               | IllegalAccessException | NoSuchMethodException e)
        {
            throw new RuntimeException("Invalid storage transport extension class specified: '" + transportExtensionClass, e);
        }
    }

    @Override
    public void close()
    {
        try
        {
            storageClient.close();
        }
        catch (Exception exception)
        {
            LOGGER.warn("Failed to close StorageClient {}", dataTransferApi.getClass().getSimpleName(), exception);
        }
    }
}
