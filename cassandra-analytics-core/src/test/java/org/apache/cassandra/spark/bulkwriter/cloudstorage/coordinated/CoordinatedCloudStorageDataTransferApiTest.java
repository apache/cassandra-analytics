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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobProgressFetchPolicy;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.RestoreJobProgressRequestParams;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobProgressResponsePayload;
import o.a.c.sidecar.client.shaded.client.RequestContext;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageDataTransferApiImpl;
import org.apache.cassandra.spark.data.QualifiedTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinatedCloudStorageDataTransferApiTest
{
    private static final String CLUSTER_ID = "cluster";
    private static final UUID JOB_ID = UUID.randomUUID();

    private SidecarClient sidecarClient;
    private CoordinatedCloudStorageDataTransferApi api;

    @BeforeEach
    void setup()
    {
        sidecarClient = mock(SidecarClient.class);
        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("MyKeyspace", "MyTable", true));
        when(jobInfo.getRestoreJobId(CLUSTER_ID)).thenReturn(JOB_ID);
        CloudStorageDataTransferApiImpl delegate = mock(CloudStorageDataTransferApiImpl.class);
        when(delegate.jobInfo()).thenReturn(jobInfo);
        when(delegate.sidecarClient()).thenReturn(sidecarClient);
        api = new CoordinatedCloudStorageDataTransferApi(mock(RateLimiter.class),
                                                         Collections.singletonMap(CLUSTER_ID, delegate));
    }

    @Test
    void testCreateRestoreSliceUsesQuotedIdentifiers()
    {
        when(sidecarClient.requestBuilder()).thenReturn(new RequestContext.Builder());
        when(sidecarClient.executeRequestAsync(any(RequestContext.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        CreateSliceRequestPayload payload = mock(CreateSliceRequestPayload.class);

        api.createRestoreSliceFromExecutor(CLUSTER_ID, payload);

        ArgumentCaptor<RequestContext> request = ArgumentCaptor.forClass(RequestContext.class);
        verify(sidecarClient).executeRequestAsync(request.capture());
        assertThat(request.getValue().request().requestURI()).contains("\"MyKeyspace\"", "\"MyTable\"", JOB_ID.toString());
        assertThat(request.getValue().request().requestBody()).isSameAs(payload);
    }

    @Test
    void testRestoreJobProgressUsesQuotedIdentifiers()
    {
        RestoreJobProgressResponsePayload response = mock(RestoreJobProgressResponsePayload.class);
        when(sidecarClient.restoreJobProgress(any(RestoreJobProgressRequestParams.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        api.restoreJobProgress(RestoreJobProgressFetchPolicy.FIRST_FAILED, cluster -> false,
                               (cluster, progress) -> {
                                   assertThat(cluster).isEqualTo(CLUSTER_ID);
                                   assertThat(progress).isSameAs(response);
                               });

        ArgumentCaptor<RestoreJobProgressRequestParams> params = ArgumentCaptor.forClass(RestoreJobProgressRequestParams.class);
        verify(sidecarClient).restoreJobProgress(params.capture());
        assertThat(params.getValue().keyspace).isEqualTo("\"MyKeyspace\"");
        assertThat(params.getValue().table).isEqualTo("\"MyTable\"");
        assertThat(params.getValue().jobId).isEqualTo(JOB_ID);
    }
}
