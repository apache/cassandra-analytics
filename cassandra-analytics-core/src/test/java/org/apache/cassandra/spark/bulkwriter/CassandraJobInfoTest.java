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

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for CassandraJobInfo class focusing on restore job ID handling
 * in single and multi-cluster scenarios.
 */
class CassandraJobInfoTest
{
    private static final String CLUSTER_1 = "cluster1";
    private static final String CLUSTER_2 = "cluster2";
    private final UUID cluster1JobId = UUID.randomUUID();
    private final UUID cluster2JobId = UUID.randomUUID();

    @Test
    void testGetRestoreJobIdInSingleClusterScenario()
    {
        MultiClusterContainer<UUID> restoreJobIds = MultiClusterContainer.ofSingle(cluster1JobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        assertThat(jobInfo.getRestoreJobId()).isEqualTo(cluster1JobId);
        assertThat(jobInfo.getRestoreJobId(null)).isEqualTo(cluster1JobId);
    }

    @Test
    void testGetRestoreJobIdWithClusterIdReturnsCorrectUuidForSpecificCluster()
    {
        MultiClusterContainer<UUID> restoreJobIds = new MultiClusterContainer<>();
        restoreJobIds.setValue(CLUSTER_1, cluster1JobId);
        restoreJobIds.setValue(CLUSTER_2, cluster2JobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        assertThat(jobInfo.getRestoreJobId(CLUSTER_1)).isEqualTo(cluster1JobId);
        assertThat(jobInfo.getRestoreJobId(CLUSTER_2)).isEqualTo(cluster2JobId);
    }

    @Test
    void testGetRestoreJobIdThrowsNoSuchElement()
    {
        MultiClusterContainer<UUID> restoreJobIds = new MultiClusterContainer<>();
        restoreJobIds.setValue(CLUSTER_1, cluster1JobId);
        restoreJobIds.setValue(CLUSTER_2, cluster2JobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        assertThatThrownBy(() -> jobInfo.getRestoreJobId(null))
        .isInstanceOf(NoSuchElementException.class);

        assertThatThrownBy(() -> jobInfo.getRestoreJobId("non-existent-cluster"))
        .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testGetRestoreJobIdFallbackToGetAnyValueWhenClusterIdIsNullAndNotFound()
    {
        MultiClusterContainer<UUID> restoreJobIds = new MultiClusterContainer<>();
        restoreJobIds.setValue(CLUSTER_1, cluster1JobId);
        restoreJobIds.setValue(CLUSTER_2, cluster2JobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        // getRestoreJobId() without parameters should fall back to getAnyValue()
        UUID resultJobId = jobInfo.getRestoreJobId();
        assertThat(resultJobId).isNotNull();
        // Should be one of the two cluster job IDs
        assertThat(resultJobId).isIn(cluster1JobId, cluster2JobId);
    }

    @Test
    void testConstructorValidationFailsWithEmptyRestoreJobIds()
    {
        MultiClusterContainer<UUID> emptyContainer = new MultiClusterContainer<>();
        BulkSparkConf mockConf = mock(BulkSparkConf.class);
        TokenPartitioner mockPartitioner = mock(TokenPartitioner.class);
        assertThatThrownBy(() -> new CassandraJobInfo(mockConf, emptyContainer, mockPartitioner))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("restoreJobIds cannot be empty");
    }

    @Test
    void testGetRestoreJobIdConsistencyAcrossMultipleCalls()
    {
        UUID expectedJobId = UUID.randomUUID();
        MultiClusterContainer<UUID> restoreJobIds = MultiClusterContainer.ofSingle(expectedJobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        // Multiple calls should return the same UUID
        Set<UUID> results = new HashSet<>();
        results.add(jobInfo.getRestoreJobId());
        results.add(jobInfo.getRestoreJobId());
        results.add(jobInfo.getRestoreJobId(null));
        assertThat(results).hasSize(1);
    }

    @Test
    void testGetRestoreJobIdWithMultiClusterConsistency()
    {
        MultiClusterContainer<UUID> restoreJobIds = new MultiClusterContainer<>();
        restoreJobIds.setValue(CLUSTER_1, cluster1JobId);
        restoreJobIds.setValue(CLUSTER_2, cluster2JobId);
        CassandraJobInfo jobInfo = createJobInfo(restoreJobIds);
        // Multiple calls for same cluster should return same UUID
        assertThat(jobInfo.getRestoreJobId(CLUSTER_1)).isEqualTo(cluster1JobId);
        assertThat(jobInfo.getRestoreJobId(CLUSTER_1)).isEqualTo(cluster1JobId);
        assertThat(jobInfo.getRestoreJobId(CLUSTER_2)).isEqualTo(cluster2JobId);
        assertThat(jobInfo.getRestoreJobId(CLUSTER_2)).isEqualTo(cluster2JobId);
        // Fallback calls should be consistent within same instance
        UUID fallback1 = jobInfo.getRestoreJobId();
        UUID fallback2 = jobInfo.getRestoreJobId();
        assertThat(fallback1).isEqualTo(fallback2);
    }

    private CassandraJobInfo createJobInfo(MultiClusterContainer<UUID> restoreJobIds)
    {
        BulkSparkConf mockConf = mock(BulkSparkConf.class);
        TokenPartitioner mockPartitioner = mock(TokenPartitioner.class);
        return new CassandraJobInfo(mockConf, restoreJobIds, mockPartitioner);
    }
}
