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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;

public class CloudStorageDataTransferApiFactory
{
    public static final CloudStorageDataTransferApiFactory INSTANCE = new CloudStorageDataTransferApiFactory();

    /**
     * Create CloudStorageDataTransferApi based on the actual runtime type of ClusterInfo
     * @return CoordinatedCloudStorageDataTransferApi if using coordinated write, i.e. with CassandraClusterInfoGroup;
     *         otherwise, CloudStorageDataTransferApiImpl
     */
    public CloudStorageDataTransferApi createDataTransferApi(StorageClient storageClient,
                                                             JobInfo jobInfo,
                                                             ClusterInfo clusterInfo)
    {
        if (clusterInfo instanceof CassandraClusterInfoGroup)
        {
            return createForCoordinated(storageClient, jobInfo, (CassandraClusterInfoGroup) clusterInfo);
        }
        else
        {
            return createForSingleCluster(storageClient, jobInfo, clusterInfo);
        }
    }

    private CloudStorageDataTransferApiImpl createForSingleCluster(StorageClient storageClient, JobInfo jobInfo, ClusterInfo clusterInfo)
    {
        return new CloudStorageDataTransferApiImpl(jobInfo,
                                                   clusterInfo.getCassandraContext().getSidecarClient(),
                                                   storageClient);
    }

    private CoordinatedCloudStorageDataTransferApi createForCoordinated(StorageClient storageClient,
                                                                        JobInfo jobInfo,
                                                                        CassandraClusterInfoGroup clusterInfoGroup)
    {
        Map<String, CloudStorageDataTransferApiImpl> apiByClusterId = new HashMap<>(clusterInfoGroup.size());
        clusterInfoGroup.forEach((clusterId, clusterInfo) -> {
            apiByClusterId.put(clusterId, createForSingleCluster(storageClient, jobInfo, clusterInfo));
        });

        return new CoordinatedCloudStorageDataTransferApi(apiByClusterId);
    }
}
