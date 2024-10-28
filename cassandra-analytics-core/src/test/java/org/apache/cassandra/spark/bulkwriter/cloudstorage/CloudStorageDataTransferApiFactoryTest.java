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

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.spark.bulkwriter.CassandraClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraClusterInfoGroup;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CloudStorageDataTransferApiFactoryTest
{
    private CloudStorageDataTransferApiFactory factory = CloudStorageDataTransferApiFactory.INSTANCE;

    @Test
    void testCreateCloudStorageDataTransferApi()
    {
        CassandraClusterInfo clusterInfo = mock(CassandraClusterInfo.class, RETURNS_DEEP_STUBS);
        when(clusterInfo.getCassandraContext().getSidecarClient()).thenReturn(mock(SidecarClient.class));
        CloudStorageDataTransferApi api = factory.createDataTransferApi(mock(StorageClient.class), mock(JobInfo.class), clusterInfo);
        assertThat(api).isInstanceOf(CloudStorageDataTransferApiImpl.class);
    }

    @Test
    void testCreateCloudStorageDataTransferApiFails()
    {
        CassandraClusterInfoGroup emptyGroup = mock(CassandraClusterInfoGroup.class);
        when(emptyGroup.size()).thenReturn(0);
        assertThatThrownBy(() -> factory.createDataTransferApi(mock(StorageClient.class), mock(JobInfo.class), emptyGroup))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("dataTransferApiByCluster cannot be empty");
    }

    @Test
    void testCreateCoordinatedCloudStorageDataTransferApi()
    {
        CassandraClusterInfo clusterInfo = mock(CassandraClusterInfo.class, RETURNS_DEEP_STUBS);
        when(clusterInfo.clusterId()).thenReturn("clusterId");
        CassandraClusterInfoGroup group = CassandraClusterInfoGroup.createFrom(Collections.singletonList(clusterInfo));
        CloudStorageDataTransferApi api = factory.createDataTransferApi(mock(StorageClient.class), mock(JobInfo.class), group);
        assertThat(api).isInstanceOf(CoordinatedCloudStorageDataTransferApi.class);
    }
}
