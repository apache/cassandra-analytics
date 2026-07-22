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

import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.data.QualifiedTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudStorageDataTransferApiImplTest
{
    private static final String QUOTED_KEYSPACE = "\"AAMBackend\"";
    private static final String QUOTED_TABLE = "\"UserBackend\"";
    private static final UUID JOB_ID = UUID.randomUUID();

    private SidecarClient sidecarClient;
    private JobInfo jobInfo;
    private CloudStorageDataTransferApiImpl api;

    @BeforeEach
    void setup()
    {
        sidecarClient = mock(SidecarClient.class);
        jobInfo = mock(JobInfo.class);
        when(jobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("AAMBackend", "UserBackend", true));
        when(jobInfo.getRestoreJobId()).thenReturn(JOB_ID);
        when(jobInfo.getRestoreJobId(null)).thenReturn(JOB_ID);
        api = new CloudStorageDataTransferApiImpl(jobInfo, sidecarClient, mock(StorageClient.class), null);
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
    void testUnquotedIdentifiersPassedAsIsWhenFlagNotSet() throws Exception
    {
        when(jobInfo.qualifiedTableName()).thenReturn(new QualifiedTableName("aambackend", "userbackend", false));
        when(sidecarClient.createRestoreJob(eq("aambackend"), eq("userbackend"), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        api.createRestoreJob(mock(CreateRestoreJobRequestPayload.class));

        verify(sidecarClient).createRestoreJob(eq("aambackend"), eq("userbackend"), any());
    }
}
