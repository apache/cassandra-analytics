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

package org.apache.cassandra.cdc.model;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.CdcKryoRegister;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.apache.cassandra.spark.utils.KryoUtils.deserialize;
import static org.apache.cassandra.spark.utils.KryoUtils.serializeToBytes;
import static org.assertj.core.api.Assertions.assertThat;

public class CdcKryoSerializationTests
{
    public static final CassandraInstance INST_1 = new CassandraInstance("0", "local1-i1", "DC1");
    public static final CassandraInstance INST_2 = new CassandraInstance("1", "local2-i1", "DC1");
    public static final CassandraInstance INST_3 = new CassandraInstance("2", "local3-i1", "DC1");

    public static final PartitionUpdateWrapper.Digest DIGEST_1 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                                   "tb1",
                                                                                                   TimeUtils.nowMicros(),
                                                                                                   new byte[]{'a', 'b', 'c'},
                                                                                                   500,
                                                                                                   BigInteger.ONE);
    public static final PartitionUpdateWrapper.Digest DIGEST_2 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                                   "tb1",
                                                                                                   TimeUtils.nowMicros(),
                                                                                                   new byte[]{'e', 'f', 'g'},
                                                                                                   10000,
                                                                                                   BigInteger.valueOf(2L));
    public static final PartitionUpdateWrapper.Digest DIGEST_3 = new PartitionUpdateWrapper.Digest("ks2",
                                                                                                   "tb1",
                                                                                                   TimeUtils.nowMicros(),
                                                                                                   new byte[]{'h', 'i', 'j'},
                                                                                                   20000,
                                                                                                   BigInteger.TEN);
    public static final PartitionUpdateWrapper.Digest DIGEST_4 = new PartitionUpdateWrapper.Digest("ks2",
                                                                                                   "tb1",
                                                                                                   TimeUtils.nowMicros(),
                                                                                                   new byte[]{'k', 'l', 'm'},
                                                                                                   30000,
                                                                                                   BigInteger.valueOf(7));


    public static Kryo kryo()
    {
        return CdcKryoRegister.kryo();
    }

    @Test
    public void testPartitionUpdateWrapperDigest()
    {
        byte[] ar = serializeToBytes(kryo(), DIGEST_1);
        PartitionUpdateWrapper.Digest deserialized = deserialize(kryo(), ar, PartitionUpdateWrapper.Digest.class);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized).isEqualTo(DIGEST_1);
        assertThat(deserialized).isNotEqualTo(DIGEST_2);
        assertThat(deserialized).isNotEqualTo(DIGEST_3);
    }

    @Test
    public void testCdcState()
    {
        testCdcStateSerialization(CdcState.BLANK);
        testCdcStateSerialization(CdcState.of(5));
        testCdcStateSerialization(CdcState.of(Integer.MAX_VALUE));
        testCdcStateSerialization(CdcState.of(Long.MAX_VALUE));
        testCdcStateSerialization(CdcState.of(Long.MAX_VALUE, TokenRange.openClosed(BigInteger.ONE, BigInteger.TEN)));
        testCdcStateSerialization(CdcState.of(Long.MAX_VALUE,
                                              TokenRange.openClosed(BigInteger.ONE, BigInteger.TEN),
                                              CommitLogMarkers.of(ImmutableMap.of(
                                              INST_1, INST_1.markerAt(1000L, 999),
                                              INST_2, INST_2.markerAt(10000L, 5000),
                                              INST_3, INST_3.markerAt(100000L, 10000)
                                              )),
                                              ImmutableMap.of(DIGEST_1, 1, DIGEST_2, 2, DIGEST_3, 3)));
    }

    private static void testCdcStateSerialization(CdcState expected)
    {
        byte[] ar = serializeToBytes(kryo(), expected);
        CdcState deserialized = deserialize(kryo(), ar, CdcState.class);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized).isEqualTo(expected);
    }

    /**
     * Regression test for the {@code ReplicaCountSerializer} short-overflow bug: the replica-count
     * map size used to be written/read as a signed short (max 32767), so a watermarker larger than
     * that (reachable via {@code CdcOptions.maxCdcStateSize()}/{@code maxWatermarkerSize()}, both
     * configurable well above 32767) would silently overflow on write and deserialize with a
     * corrupted (potentially negative) size, observed in production as a permanent
     * restart-crash-loop ({@code IllegalArgumentException: Illegal initial capacity}). The map size
     * is now a signed int, so this must round-trip correctly for a map with more than 32767 entries.
     */
    @Test
    public void testCdcStateReplicaCountMapLargerThanShortOverflows()
    {
        int size = Short.MAX_VALUE + 1000; // comfortably past the old 32767 short limit
        Map<PartitionUpdateWrapper.Digest, Integer> replicaCount = new HashMap<>(size);
        for (int i = 0; i < size; i++)
        {
            PartitionUpdateWrapper.Digest digest = new PartitionUpdateWrapper.Digest("ks1",
                                                                                      "tb1",
                                                                                      TimeUtils.nowMicros(),
                                                                                      intToBytes(i),
                                                                                      500,
                                                                                      BigInteger.ONE);
            replicaCount.put(digest, i % 128); // replica count is serialized as a single byte
        }

        CdcState expected = CdcState.of(1L,
                                        TokenRange.openClosed(BigInteger.ONE, BigInteger.TEN),
                                        CommitLogMarkers.EMPTY,
                                        replicaCount);
        assertThat(expected.size()).isEqualTo(size);

        testCdcStateSerialization(expected);
    }

    private static byte[] intToBytes(int value)
    {
        return new byte[]{
        (byte) (value >>> 24),
        (byte) (value >>> 16),
        (byte) (value >>> 8),
        (byte) value
        };
    }
}
