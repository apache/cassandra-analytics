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

package org.apache.cassandra.cdc.state;

import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.DIGEST_1;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.DIGEST_2;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.DIGEST_3;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.DIGEST_4;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.INST_1;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.INST_2;
import static org.apache.cassandra.cdc.model.CdcKryoSerializationTests.INST_3;
import static org.assertj.core.api.Assertions.assertThat;

public class CdcStateTests
{
    @Test
    public void testMergeEpoch()
    {
        CdcState state1 = CdcState.of(500L);
        CdcState state2 = CdcState.of(1000L);
        CdcState merged = state1.merge(TokenRange.openClosed(BigInteger.ONE, BigInteger.TEN), state2);
        assertThat(merged.epoch).isEqualTo(1000);
    }

    /**
     * Shrink test: state from two token ranges merge into one
     */
    @Test
    public void testShrink()
    {
        CdcState state1 = CdcState.of(500L,
                                      TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN),
                                      CommitLogMarkers.of(ImmutableMap.of(
                                      INST_1, INST_1.markerAt(1000L, 100),
                                      INST_2, INST_2.markerAt(2000L, 200),
                                      INST_3, INST_3.markerAt(3000L, 300)
                                      )),
                                      ImmutableMap.of(DIGEST_1, 1, DIGEST_2, 2, DIGEST_3, 3));

        CdcState state2 = CdcState.of(1000L,
                                      TokenRange.openClosed(BigInteger.TEN, BigInteger.valueOf(20)),
                                      CommitLogMarkers.of(ImmutableMap.of(
                                      INST_1, INST_1.markerAt(4000L, 400),
                                      INST_2, INST_2.markerAt(5000L, 500),
                                      INST_3, INST_3.markerAt(6000L, 600)
                                      )),
                                      ImmutableMap.of(DIGEST_1, 1, DIGEST_2, 1, DIGEST_3, 1, DIGEST_4, 1));

        CdcState merged = state1.merge(TokenRange.openClosed(BigInteger.ZERO, BigInteger.valueOf(20)), state2);
        assertThat(merged.epoch).isEqualTo(Math.max(state1.epoch, state2.epoch));
        assertThat(merged.range).isEqualTo(TokenRange.openClosed(BigInteger.ZERO, BigInteger.valueOf(20)));

        // Verify we maintain the token history.
        // We should ignore commit log markers that have already been read per token range.
        assertThat(merged.markers.canIgnore(INST_1.markerAt(999L, 150), BigInteger.ONE)).isTrue();
        assertThat(merged.markers.canIgnore(INST_1.markerAt(1000L, 50), BigInteger.ONE)).isTrue();
        assertThat(merged.markers.canIgnore(INST_1.markerAt(1000L, 150), BigInteger.ONE)).isFalse();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(2000L, 150), BigInteger.valueOf(5))).isTrue();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(2000L, 225), BigInteger.valueOf(5))).isFalse();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(2000L, 150), BigInteger.valueOf(7))).isTrue();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(2500L, 50), BigInteger.valueOf(7))).isFalse();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(2500L, 150), BigInteger.valueOf(7))).isTrue();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(3000L, 150), BigInteger.valueOf(7))).isTrue();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(3000L, 305), BigInteger.valueOf(7))).isFalse();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(3000L, 305), BigInteger.TEN)).isFalse();

        assertThat(merged.markers.canIgnore(INST_1.markerAt(3999L, 450), BigInteger.valueOf(11))).isTrue();
        assertThat(merged.markers.canIgnore(INST_1.markerAt(4000L, 350), BigInteger.valueOf(15))).isTrue();
        assertThat(merged.markers.canIgnore(INST_1.markerAt(4000L, 550), BigInteger.valueOf(15))).isFalse();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(5000L, 450), BigInteger.valueOf(15))).isTrue();
        assertThat(merged.markers.canIgnore(INST_2.markerAt(5000L, 550), BigInteger.valueOf(15))).isFalse();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(5000L, 900), BigInteger.valueOf(20))).isTrue();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(6000L, 600), BigInteger.valueOf(20))).isFalse();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(6000L, 599), BigInteger.valueOf(20))).isTrue();
        assertThat(merged.markers.canIgnore(INST_3.markerAt(6000L, 650), BigInteger.valueOf(20))).isFalse();

        // verify it takes the max replica count for each mutation digest
        assertThat(merged.replicaCount.get(DIGEST_1)).isEqualTo(1);
        assertThat(merged.replicaCount.get(DIGEST_2)).isEqualTo(2);
        assertThat(merged.replicaCount.get(DIGEST_3)).isEqualTo(3);
        assertThat(merged.replicaCount.get(DIGEST_4)).isEqualTo(1);
    }

    @Test
    public void testPurge()
    {
        long now = TimeUtils.nowMicros();
        long maxAgeMicros = 10;
        PartitionUpdateWrapper.Digest digest1 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now,
                                                                                  new byte[]{'a', 'b', 'c'},
                                                                                  500,
                                                                                  BigInteger.ONE);
        PartitionUpdateWrapper.Digest digest2 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now - 5,
                                                                                  new byte[]{'d', 'e', 'f'},
                                                                                  500,
                                                                                  BigInteger.valueOf(2L));
        PartitionUpdateWrapper.Digest digest3 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now - maxAgeMicros - 1,
                                                                                  new byte[]{'g', 'h', 'i'},
                                                                                  500,
                                                                                  BigInteger.valueOf(8L));
        CdcState startState = CdcState.of(500L,
                                          TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN),
                                          CommitLogMarkers.of(ImmutableMap.of(
                                          INST_1, INST_1.markerAt(1000L, 100),
                                          INST_2, INST_2.markerAt(2000L, 200),
                                          INST_3, INST_3.markerAt(3000L, 300)
                                          )),
                                          ImmutableMap.of(digest1, 1, digest2, 2, digest3, 3));
        assertThat(startState.size()).isEqualTo(3);
        CdcState endState = startState.mutate().purge(CdcStats.STUB, now - maxAgeMicros).build();
        assertThat(endState.size()).isEqualTo(2);
        assertThat(endState.epoch).isEqualTo(startState.epoch);
        assertThat(endState.markers).isEqualTo(startState.markers);
        assertThat(endState.range).isEqualTo(startState.range);
        assertThat(endState.replicaCount).isNotEqualTo(startState.replicaCount);
    }

    @Test
    public void testPurgeIfFull()
    {
        long now = TimeUtils.nowMicros();
        long maxAgeMicros = 10;
        PartitionUpdateWrapper.Digest digest1 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now,
                                                                                  new byte[]{'a', 'b', 'c'},
                                                                                  500,
                                                                                  BigInteger.ONE);
        PartitionUpdateWrapper.Digest digest2 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now - 5,
                                                                                  new byte[]{'d', 'e', 'f'},
                                                                                  500,
                                                                                  BigInteger.valueOf(2L));
        PartitionUpdateWrapper.Digest digest3 = new PartitionUpdateWrapper.Digest("ks1",
                                                                                  "tb1",
                                                                                  now - maxAgeMicros - 1,
                                                                                  new byte[]{'g', 'h', 'i'},
                                                                                  500,
                                                                                  BigInteger.valueOf(8L));
        CdcState startState = CdcState.of(500L,
                                     TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN),
                                     CommitLogMarkers.of(ImmutableMap.of(
                                     INST_1, INST_1.markerAt(1000L, 100),
                                     INST_2, INST_2.markerAt(2000L, 200),
                                     INST_3, INST_3.markerAt(3000L, 300)
                                     )),
                                     ImmutableMap.of(digest1, 1, digest2, 2, digest3, 3));
        assertThat(startState.size()).isEqualTo(3);
        CdcState endState = startState.purgeIfFull(CdcStats.STUB, new CdcOptions()
        {
            @Override
            public long microsecondProvider()
            {
                return now;
            }

            public int maxCdcStateSize()
            {
                return 3;
            }

            @Override
            public Duration maximumAge()
            {
                return Duration.ofNanos(TimeUnit.MICROSECONDS.toNanos(maxAgeMicros));
            }
        });

        assertThat(endState.size()).isEqualTo(2);
        assertThat(endState.epoch).isEqualTo(startState.epoch);
        assertThat(endState.markers).isEqualTo(startState.markers);
        assertThat(endState.range).isEqualTo(startState.range);
        assertThat(endState.replicaCount).isNotEqualTo(startState.replicaCount);
    }
}
