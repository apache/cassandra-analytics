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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import o.a.c.sidecar.client.shaded.client.RequestContext;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.data.QualifiedTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudStorageDataTransferApiImplTest
{
    private static final String QUOTED_KEYSPACE = "\"MyKeyspace\"";
    private static final String QUOTED_TABLE = "\"MyTable\"";
    private static final UUID JOB_ID = UUID.randomUUID();

    private SidecarClient sidecarClient;
    private JobInfo jobInfo;
    private CloudStorageDataTransferApiImpl api;

    @BeforeEach
    void setup()
    {
        sidecarClient = mock(SidecarClient.class);
        jobInfo = mock(JobInfo.class);
        when(jobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("MyKeyspace", "MyTable", true));
        when(jobInfo.getRestoreJobId()).thenReturn(JOB_ID);
        when(jobInfo.getRestoreJobId(null)).thenReturn(JOB_ID);
        api = new CloudStorageDataTransferApiImpl(jobInfo, CassandraBridgeFactory.get(CassandraVersion.FOURZERO),
                                                  sidecarClient, mock(StorageClient.class), null);
    }

    @Test
    void testCreateRestoreJobUsesQuotedIdentifiers() throws Exception
    {
        when(sidecarClient.createRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        api.createRestoreJob(mock(CreateRestoreJobRequestPayload.class));

        verify(sidecarClient).createRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), any());
    }

    @Test
    void testRestoreJobSummaryUsesQuotedIdentifiers() throws Exception
    {
        RestoreJobSummaryResponsePayload response = mock(RestoreJobSummaryResponsePayload.class);
        when(sidecarClient.restoreJobSummary(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID)))
            .thenReturn(CompletableFuture.completedFuture(response));

        RestoreJobSummaryResponsePayload result = api.restoreJobSummary();

        assertThat(result).isSameAs(response);
        verify(sidecarClient).restoreJobSummary(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID));
    }

    @Test
    void testUpdateRestoreJobUsesQuotedIdentifiers() throws Exception
    {
        when(sidecarClient.updateRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        api.updateRestoreJob(mock(UpdateRestoreJobRequestPayload.class));

        verify(sidecarClient).updateRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID), any());
    }

    @Test
    void testAbortRestoreJobUsesQuotedIdentifiers() throws Exception
    {
        when(sidecarClient.abortRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID)))
            .thenReturn(CompletableFuture.completedFuture(null));

        api.abortRestoreJob();

        verify(sidecarClient).abortRestoreJob(eq(QUOTED_KEYSPACE), eq(QUOTED_TABLE), eq(JOB_ID));
    }

    @Test
    void testRestoreSliceUsesQuotedIdentifiers() throws Exception
    {
        when(sidecarClient.requestBuilder()).thenReturn(new RequestContext.Builder());
        when(sidecarClient.executeRequestAsync(any(RequestContext.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        SidecarInstance instance = mock(SidecarInstance.class);
        CreateSliceRequestPayload payload = mock(CreateSliceRequestPayload.class);

        api.createRestoreSliceFromExecutor(instance, payload);
        api.createRestoreSliceFromDriver(instance, payload).get();

        ArgumentCaptor<RequestContext> requests = ArgumentCaptor.forClass(RequestContext.class);
        verify(sidecarClient, times(2)).executeRequestAsync(requests.capture());
        for (RequestContext request : requests.getAllValues())
        {
            assertThat(request.request().requestURI()).contains(QUOTED_KEYSPACE, QUOTED_TABLE, JOB_ID.toString());
            assertThat(request.request().requestBody()).isSameAs(payload);
        }
    }

    @ParameterizedTest
    @CsvSource({
    "false, mykeyspace, mytable, mykeyspace, mytable",
    "false, MyKeyspace, MyTable, MyKeyspace, MyTable",
    "true, mykeyspace, mytable, mykeyspace, mytable",
    "true, MyKeyspace, MyTable, \"MyKeyspace\", \"MyTable\"",
    "true, select, table, \"select\", \"table\""
    })
    void testRestoreApisUseBridgeQuoting(boolean quoteIdentifiers,
                                         String keyspace,
                                         String table,
                                         String expectedKeyspace,
                                         String expectedTable) throws Exception
    {
        when(jobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName(keyspace, table, quoteIdentifiers));
        when(sidecarClient.createRestoreJob(eq(expectedKeyspace), eq(expectedTable), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(sidecarClient.restoreJobSummary(expectedKeyspace, expectedTable, JOB_ID))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(sidecarClient.updateRestoreJob(eq(expectedKeyspace), eq(expectedTable), eq(JOB_ID), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(sidecarClient.abortRestoreJob(expectedKeyspace, expectedTable, JOB_ID))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(sidecarClient.requestBuilder()).thenReturn(new RequestContext.Builder());
        when(sidecarClient.executeRequestAsync(any(RequestContext.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        api.createRestoreJob(mock(CreateRestoreJobRequestPayload.class));
        api.restoreJobSummary();
        api.updateRestoreJob(mock(UpdateRestoreJobRequestPayload.class));
        api.abortRestoreJob();
        SidecarInstance instance = mock(SidecarInstance.class);
        CreateSliceRequestPayload payload = mock(CreateSliceRequestPayload.class);
        api.createRestoreSliceFromExecutor(instance, payload);
        api.createRestoreSliceFromDriver(instance, payload).get();

        verify(sidecarClient).createRestoreJob(eq(expectedKeyspace), eq(expectedTable), any());
        verify(sidecarClient).restoreJobSummary(expectedKeyspace, expectedTable, JOB_ID);
        verify(sidecarClient).updateRestoreJob(eq(expectedKeyspace), eq(expectedTable), eq(JOB_ID), any());
        verify(sidecarClient).abortRestoreJob(expectedKeyspace, expectedTable, JOB_ID);
        ArgumentCaptor<RequestContext> requests = ArgumentCaptor.forClass(RequestContext.class);
        verify(sidecarClient, times(2)).executeRequestAsync(requests.capture());
        for (RequestContext request : requests.getAllValues())
        {
            assertThat(request.request().requestURI())
            .isEqualTo("/api/v1/keyspaces/" + expectedKeyspace + "/tables/" + expectedTable + "/restore-jobs/" + JOB_ID + "/slices");
            assertThat(request.request().requestBody()).isSameAs(payload);
        }
    }
}
