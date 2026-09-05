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

package org.apache.cassandra.cdc.sidecar;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.data.CdcSegmentInfo;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for SidecarCdcCommitLogSegment class
 */
public class SidecarCdcCommitLogSegmentTest
{
    /**
     * Test that maxOffset() and source().size() return segment.size instead of segment.idx.
     * This prevents AssertionError in CdcRandomAccessReader when buffer calculations expect
     * the wrong file size, causing the assertion "buffer.remaining() == 0" to fail.
     */
    @Test
    public void testMaxOffsetAndSizeReturnFileSizeNotSegmentId()
    {
        // Use real values from the bug: file size 83544 bytes, segment ID 1770159718743
        long fileSize = 83544L;
        long segmentId = 1770159718743L;

        CdcSegmentInfo segmentInfo = new CdcSegmentInfo(
            "CommitLog-7-1770159718743.log",
            fileSize,
            segmentId,
            false,
            System.currentTimeMillis()
        );

        // Create test stub with stats field
        TestSidecarCdcClient testSidecarClient = new TestSidecarCdcClient();
        SidecarCdcClient.ClientConfig clientConfig = SidecarCdcClient.ClientConfig.create();

        SidecarCdcCommitLogSegment segment = new SidecarCdcCommitLogSegment(
            testSidecarClient,
            mock(CassandraInstance.class),
            segmentInfo,
            clientConfig
        );

        // maxOffset() and source().size() must return file size, not segment ID
        assertThat(segment.maxOffset()).isEqualTo(fileSize);
        assertThat(segment.source().size()).isEqualTo(fileSize);
    }

    /**
     * Test stub for SidecarCdcClient
     */
    private static class TestSidecarCdcClient extends SidecarCdcClient
    {
        TestSidecarCdcClient()
        {
            super(null, null, ICdcStats.STUB);
        }
    }
}
