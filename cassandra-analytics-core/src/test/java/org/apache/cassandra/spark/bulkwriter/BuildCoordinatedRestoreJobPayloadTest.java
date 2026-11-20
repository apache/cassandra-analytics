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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.SimpleClusterConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel.CL;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CassandraBulkSourceRelation#buildCoordinatedRestoreJobPayload}, i.e. the create restore job
 * request sent to each cluster of a coordinated write.
 */
class BuildCoordinatedRestoreJobPayloadTest
{
    private static final String CLUSTER_ID = "cluster1";
    // restore job ids must be time based (version 1) UUIDs
    private static final UUID JOB_ID = UUID.fromString("0846b690-ce35-11e7-8871-79b4d1aa8ef2");

    @Test
    void testFastForwardEnabledIsSentToSidecar()
    {
        SimpleClusterConf cluster = new SimpleClusterConf(Collections.singletonList("instance-1:9043"), "dc1");
        CreateRestoreJobRequestPayload payload = buildPayload(cluster, true);
        assertThat(payload.fastForwardEnabled()).isTrue();
        assertThat(payload.jobId()).isEqualTo(JOB_ID);
        assertThat(payload.consistencyLevel()).isEqualTo("LOCAL_QUORUM");
        assertThat(payload.localDatacenter()).isEqualTo("dc1");
    }

    @Test
    void testFastForwardDisabled()
    {
        SimpleClusterConf cluster = new SimpleClusterConf(Collections.singletonList("instance-1:9043"), "dc1");
        CreateRestoreJobRequestPayload payload = buildPayload(cluster, false);
        assertThat(payload.fastForwardEnabled()).isFalse();
    }

    @Test
    void testWriteToLocalDcOnlyIsSentToSidecar()
    {
        SimpleClusterConf cluster = new SimpleClusterConf(Collections.singletonList("instance-1:9043"), "dc1", true);
        CreateRestoreJobRequestPayload payload = buildPayload(cluster, false);
        assertThat(payload.shouldRestoreToLocalDatacenterOnly()).isTrue();
        assertThat(payload.fastForwardEnabled()).isFalse();
    }

    private static CreateRestoreJobRequestPayload buildPayload(SimpleClusterConf cluster, boolean fastForwardEnabled)
    {
        JobInfo job = mock(JobInfo.class);
        when(job.getRestoreJobId(CLUSTER_ID)).thenReturn(JOB_ID);
        return CassandraBulkSourceRelation.buildCoordinatedRestoreJobPayload(job,
                                                                             CLUSTER_ID,
                                                                             cluster,
                                                                             fastForwardEnabled,
                                                                             CL.LOCAL_QUORUM,
                                                                             secrets(),
                                                                             null,
                                                                             System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10));
    }

    private static RestoreJobSecrets secrets()
    {
        return new StorageCredentialPair("eu-west-1",
                                         new StorageCredentials("writeKey", "writeSecret", "writeToken"),
                                         "us-east-1",
                                         new StorageCredentials("readKey", "readSecret", "readToken"))
               .toRestoreJobSecrets();
    }
}
