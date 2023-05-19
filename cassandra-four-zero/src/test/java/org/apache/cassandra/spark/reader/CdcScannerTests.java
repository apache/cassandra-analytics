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

package org.apache.cassandra.spark.reader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.stats.Stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CdcScannerTests
{
    private Watermarker watermarker(boolean isLate)
    {
        Watermarker watermarker = mock(Watermarker.class);
        when(watermarker.seenBefore(any(PartitionUpdateWrapper.class))).thenReturn(isLate);
        return watermarker;
    }

    private static PartitionUpdateWrapper update(long timestamp)
    {
        PartitionUpdateWrapper update = mock(PartitionUpdateWrapper.class);
        when(update.maxTimestampMicros()).thenReturn(timestamp * 1000L);  // In microseconds
        return update;
    }

    private void test(List<PartitionUpdateWrapper> updates,
                      Watermarker watermarker,
                      int minimumReplicasPerMutation,
                      boolean shouldPublish)
    {
        assertEquals(shouldPublish, CdcScannerBuilder.filter(updates, minimumReplicasPerMutation, watermarker, Stats.DoNothingStats.INSTANCE));
    }

    @Test
    public void testPublishedClAll()
    {
        Watermarker watermarker = watermarker(false);
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        PartitionUpdateWrapper update1 = update(now);
        PartitionUpdateWrapper update2 = update(now);
        PartitionUpdateWrapper update3 = update(now);
        List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2, update3);
        test(updates, watermarker, 3, true);
        for (PartitionUpdateWrapper update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testPublishedClQuorum()
    {
        Watermarker watermarker = watermarker(false);
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        PartitionUpdateWrapper update1 = update(now);
        PartitionUpdateWrapper update2 = update(now);
        List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, 2, true);
        for (PartitionUpdateWrapper update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testInsufficientReplicas()
    {
        Watermarker watermarker = watermarker(false);
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        PartitionUpdateWrapper update1 = update(now);
        List<PartitionUpdateWrapper> updates = Collections.singletonList(update1);
        test(updates, watermarker, 2, false);
        for (PartitionUpdateWrapper update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testInsufficientReplicasLate()
    {
        Watermarker watermarker = watermarker(false);
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        PartitionUpdateWrapper update1 = update(now);
        List<PartitionUpdateWrapper> updates = Collections.singletonList(update1);
        test(updates, watermarker, 2, false);
        for (PartitionUpdateWrapper update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testLateMutation()
    {
        Watermarker watermarker = watermarker(true);
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        PartitionUpdateWrapper update1 = update(now);
        PartitionUpdateWrapper update2 = update(now);
        List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, 2, true);
        verify(watermarker).untrackReplicaCount(eq(update1));
    }

    @Test
    public void testCommitLogFilename()
    {
        assertEquals(12345L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-4-12345.log")).longValue());
        assertEquals(12345L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-12345.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-7-1646094405659.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-1646094405659.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-242-1646094405659.log")).longValue());
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog-123-abcd.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog-abcd.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("abcd"));
    }
}
