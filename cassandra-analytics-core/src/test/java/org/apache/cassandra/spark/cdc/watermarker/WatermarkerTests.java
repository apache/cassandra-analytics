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

package org.apache.cassandra.spark.cdc.watermarker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatermarkerTests
{
    private static final String JOB_ID = "101";

    private static IPartitionUpdateWrapper partitionUpdate(long timestamp)
    {
        IPartitionUpdateWrapper update = mock(IPartitionUpdateWrapper.class);
        when(update.maxTimestampMicros()).thenReturn(timestamp * 1000L);  // In microseconds
        return update;
    }

    @BeforeAll
    public static void setup()
    {
        InMemoryWatermarker.TEST_THREAD_NAME = Thread.currentThread().getName();
    }

    @AfterAll
    public static void tearDown()
    {
        InMemoryWatermarker.TEST_THREAD_NAME = null;
    }

    @Test
    public void testHighwaterMark() throws ExecutionException, InterruptedException
    {
        Watermarker watermarker = InMemoryWatermarker.INSTANCE.instance(JOB_ID);
        watermarker.clear();

        assertEquals(watermarker, InMemoryWatermarker.INSTANCE.instance(JOB_ID));
        InMemoryWatermarker.PartitionWatermarker partitionWatermarker = (InMemoryWatermarker.PartitionWatermarker) watermarker.instance(JOB_ID);
        assertEquals(partitionWatermarker, partitionWatermarker.instance(JOB_ID));

        // Calling from another thread should result in NPE
        AtomicReference<Boolean> pass = new AtomicReference<>(false);
        TestDataLayer.FILE_IO_EXECUTOR.submit(() -> {
            try
            {
                InMemoryWatermarker.INSTANCE.instance(JOB_ID);
                pass.set(false);
            }
            catch (NullPointerException exception)
            {
                pass.set(true);
            }
        }).get();
        assertTrue(pass.get());

        CassandraInstance in1 = new CassandraInstance("0L", "inst1", "DC1");
        CassandraInstance in2 = new CassandraInstance("100L", "inst2", "DC1");

        assertNull(watermarker.highWaterMark(in1));
        assertNull(watermarker.highWaterMark(in2));

        // Verify highwater mark tracks the highest seen
        for (int index = 0; index <= 100; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 1L, 10 * index));
        }
        assertEquals(new CommitLog.Marker(in1, 1L, 1000), watermarker.highWaterMark(in1));
        assertNull(watermarker.highWaterMark(in2));

        watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 2L, 1));
        assertEquals(new CommitLog.Marker(in1, 2L, 1), watermarker.highWaterMark(in1));
        for (int index = 0; index <= 100; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 2L, 5 * index));
        }
        assertEquals(new CommitLog.Marker(in1, 2L, 500), watermarker.highWaterMark(in1));

        for (int index = 0; index <= 100; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 1L, 5 * index));
        }
        assertEquals(new CommitLog.Marker(in1, 2L, 500), watermarker.highWaterMark(in1));
    }

    @Test
    public void testLateMutation()
    {
        Watermarker watermarker = InMemoryWatermarker.INSTANCE.instance(JOB_ID);
        watermarker.clear();

        CassandraInstance in1 = new CassandraInstance("0L", "inst1", "DC1");
        for (int index = 0; index <= 100; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 2L, 5 * index));
        }
        for (int index = 0; index <= 100; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 10L, 5 * index));
        }
        CommitLog.Marker end = new CommitLog.Marker(in1, 10L, 500);
        assertEquals(end, watermarker.highWaterMark(in1));

        // Verify late mutations track earliest marker
        long now = System.currentTimeMillis();
        IPartitionUpdateWrapper update1 = partitionUpdate(now);
        watermarker.recordReplicaCount(update1, 2);
        assertEquals(end, watermarker.highWaterMark(in1));
        IPartitionUpdateWrapper update2 = partitionUpdate(now);
        watermarker.recordReplicaCount(update2, 2);
        assertEquals(end, watermarker.highWaterMark(in1));
        IPartitionUpdateWrapper update3 = partitionUpdate(now);
        watermarker.recordReplicaCount(update3, 2);
        assertEquals(end, watermarker.highWaterMark(in1));
        IPartitionUpdateWrapper update4 = partitionUpdate(now);
        watermarker.recordReplicaCount(update4, 2);

        assertEquals(end, watermarker.highWaterMark(in1));
        for (int index = 101; index <= 200; index++)
        {
            watermarker.updateHighWaterMark(new CommitLog.Marker(in1, 10L, 5 * index));
        }
        end = new CommitLog.Marker(in1, 10L, 1000);
        assertEquals(end, watermarker.highWaterMark(in1));

        assertTrue(watermarker.seenBefore(update1));
        assertTrue(watermarker.seenBefore(update2));
        assertTrue(watermarker.seenBefore(update3));
        assertTrue(watermarker.seenBefore(update4));
        assertEquals(2, watermarker.replicaCount(update1));
        assertEquals(2, watermarker.replicaCount(update2));
        assertEquals(2, watermarker.replicaCount(update3));
        assertEquals(2, watermarker.replicaCount(update4));

        // Clear mutations and verify watermark tracks last offset in order
        watermarker.untrackReplicaCount(update2);
        watermarker.untrackReplicaCount(update3);
        watermarker.untrackReplicaCount(update4);
        watermarker.untrackReplicaCount(update1);
        assertEquals(end, watermarker.highWaterMark(in1));

        assertEquals(0, watermarker.replicaCount(update1));
        assertEquals(0, watermarker.replicaCount(update2));
        assertEquals(0, watermarker.replicaCount(update3));
        assertEquals(0, watermarker.replicaCount(update4));
    }

    @Test
    public void testPublishedMutation()
    {
        Watermarker watermarker = InMemoryWatermarker.INSTANCE.instance(JOB_ID);
        watermarker.clear();
        CassandraInstance in1 = new CassandraInstance("0L", "inst1", "DC1");
        long now = System.currentTimeMillis();
        CommitLog.Marker end = new CommitLog.Marker(in1, 5L, 600);
        watermarker.updateHighWaterMark(end);

        IPartitionUpdateWrapper lateUpdate1 = partitionUpdate(now);
        watermarker.recordReplicaCount(lateUpdate1, 2);
        IPartitionUpdateWrapper lateUpdate2 = partitionUpdate(now);
        watermarker.recordReplicaCount(lateUpdate2, 2);
        IPartitionUpdateWrapper lateUpdate3 = partitionUpdate(now);
        watermarker.recordReplicaCount(lateUpdate3, 2);

        assertEquals(end, watermarker.highWaterMark(in1));

        watermarker.untrackReplicaCount(lateUpdate1);
        watermarker.untrackReplicaCount(lateUpdate2);
        watermarker.untrackReplicaCount(lateUpdate3);

        // Back at the highwater marker so published & late mutation markers have been cleared
        assertEquals(end, watermarker.highWaterMark(in1));
    }
}
