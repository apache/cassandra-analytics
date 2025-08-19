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

package org.apache.cassandra.cdc.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommitLogMarkerTests
{
    @Test
    public void testEmpty()
    {
        CassandraInstance inst = new CassandraInstance("0", "local1-i1", "DC1");
        Marker marker = CommitLogMarkers.EMPTY.startMarker(inst);
        assertThat(marker.segmentId).isEqualTo(0);
        assertThat(marker.position).isEqualTo(0);
        assertThat(CommitLogMarkers.EMPTY.canIgnore(inst.zeroMarker(), BigInteger.ZERO)).isFalse();
        assertThat(CommitLogMarkers.EMPTY.canIgnore(inst.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), Partitioner.Murmur3Partitioner.maxToken())).isFalse();
        assertThat(CommitLogMarkers.EMPTY.canIgnore(inst.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), Partitioner.Murmur3Partitioner.minToken())).isFalse();
    }

    @Test
    public void testPerInstance()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
        CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
        CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");

        CommitLogMarkers markers = CommitLogMarkers.of(
        ImmutableMap.of(
        inst1, inst1.markerAt(500, 10000),
        inst2, inst2.markerAt(99999, 0),
        inst3, inst3.markerAt(10000000, 120301312)
        )
        );

        assertThat(markers.startMarker(inst1)).isEqualTo(inst1.markerAt(500, 10000));
        assertThat(markers.startMarker(inst2)).isEqualTo(inst2.markerAt(99999, 0));
        assertThat(markers.startMarker(inst3)).isEqualTo(inst3.markerAt(10000000, 120301312));

        assertThat(markers.canIgnore(inst1.zeroMarker(), BigInteger.ZERO)).isFalse();
        assertThat(markers.canIgnore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), BigInteger.ZERO)).isFalse();
    }

    @Test
    public void testPerRange()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
        CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
        CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");

        // build per range commit log markers
        PerRangeCommitLogMarkers.PerRangeBuilder builder = CommitLogMarkers.perRangeBuilder();
        builder.add(TokenRange.closed(BigInteger.ZERO, BigInteger.valueOf(5000)), inst1.markerAt(500, 10000));
        builder.add(TokenRange.closed(BigInteger.valueOf(5000), BigInteger.valueOf(10000)), inst1.markerAt(600, 20000));
        builder.add(TokenRange.closed(BigInteger.valueOf(10000), BigInteger.valueOf(15000)), inst2.markerAt(99999, 0));
        builder.add(TokenRange.closed(BigInteger.valueOf(15000), BigInteger.valueOf(20000)), inst2.markerAt(2000, 500));
        builder.add(TokenRange.closed(BigInteger.valueOf(20000), BigInteger.valueOf(25000)), inst3.markerAt(0, 0));
        builder.add(TokenRange.closed(BigInteger.valueOf(25000), BigInteger.valueOf(30000)), inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE));
        builder.add(TokenRange.closed(BigInteger.valueOf(20000), BigInteger.valueOf(30000)), inst3.markerAt(500, 500));
        PerRangeCommitLogMarkers markers = builder.build();

        // verify start marker is the min
        assertThat(markers.startMarker(inst1)).isEqualTo(inst1.markerAt(500, 10000));
        assertThat(markers.startMarker(inst2)).isEqualTo(inst2.markerAt(2000, 500));
        assertThat(markers.startMarker(inst3)).isEqualTo(inst3.zeroMarker());

        // verify CommitLog positions we can/can't ignore on instance 1
        assertThat(markers.canIgnore(inst1.markerAt(400, 0), BigInteger.ZERO)).isTrue();
        assertThat(markers.canIgnore(inst1.markerAt(400, 0), BigInteger.ONE)).isTrue();
        assertThat(markers.canIgnore(inst1.markerAt(500, 0), BigInteger.ONE)).isTrue();
        assertThat(markers.canIgnore(inst1.markerAt(500, 10000), BigInteger.ZERO)).isFalse();
        assertThat(markers.canIgnore(inst1.markerAt(500, 10000), BigInteger.ONE)).isFalse();
        assertThat(markers.canIgnore(inst1.markerAt(500, 10001), BigInteger.ONE)).isFalse();
        assertThat(markers.canIgnore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), BigInteger.ONE)).isFalse();

        // verify CommitLog positions we can/can't ignore on instance 2
        assertThat(markers.canIgnore(inst2.zeroMarker(), BigInteger.ZERO)).isFalse();
        assertThat(markers.canIgnore(inst2.zeroMarker(), BigInteger.valueOf(7000))).isFalse();
        assertThat(markers.canIgnore(inst2.zeroMarker(), BigInteger.valueOf(11000))).isTrue();
        assertThat(markers.canIgnore(inst2.markerAt(99998, Integer.MAX_VALUE), BigInteger.valueOf(11000))).isTrue();
        assertThat(markers.canIgnore(inst2.markerAt(99999, 0), BigInteger.valueOf(11000))).isFalse();

        // verify CommitLog positions we can/can't ignore on instance 3
        assertThat(markers.canIgnore(inst3.zeroMarker(), BigInteger.ZERO)).isFalse();
        assertThat(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(20000))).isFalse();
        assertThat(markers.canIgnore(inst3.zeroMarker(), BigInteger.valueOf(20000))).isTrue();
        assertThat(markers.canIgnore(inst3.markerAt(500, 499), BigInteger.valueOf(20000))).isTrue();
        assertThat(markers.canIgnore(inst3.markerAt(500, 500), BigInteger.valueOf(20000))).isFalse();
        assertThat(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(25000))).isTrue();
        assertThat(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(30000))).isTrue();
    }

    @Test
    public void testIsBefore()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");

        assertThat(inst1.zeroMarker().isBefore(inst1.zeroMarker())).isFalse();

        assertThat(inst1.zeroMarker().isBefore(inst1.markerAt(0, 1))).isTrue();
        assertThat(inst1.zeroMarker().isBefore(inst1.markerAt(1, 1))).isTrue();
        assertThat(inst1.markerAt(1, 0).isBefore(inst1.markerAt(1, 1))).isTrue();
        assertThat(inst1.markerAt(10000, Integer.MAX_VALUE).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE))).isTrue();
        assertThat(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE))).isTrue();

        assertThat(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE).isBefore(inst1.zeroMarker())).isFalse();
        assertThat(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE))).isFalse();
        assertThat(inst1.markerAt(10000, 5001).isBefore(inst1.markerAt(10000, 5000))).isFalse();
        assertThat(inst1.markerAt(10001, 5000).isBefore(inst1.markerAt(10000, 5000))).isFalse();
    }

    @Test
    public void testIsBeforeException()
    {
        assertThatThrownBy(() -> {
            CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
            CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
            inst1.zeroMarker().isBefore(inst2.zeroMarker());
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPerInstanceJdkSerialization()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
        CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
        CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");

        CommitLogMarkers markers = CommitLogMarkers.of(
        ImmutableMap.of(
        inst1, inst1.markerAt(500, 10000),
        inst2, inst2.markerAt(99999, 0),
        inst3, inst3.markerAt(10000000, 120301312)
        )
        );

        byte[] ar = serialize(markers);
        CommitLogMarkers deserialized = deserialize(ar, PerInstanceCommitLogMarkers.class);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized).isEqualTo(markers);
        assertThat(deserialized.startMarker(inst1)).isEqualTo(inst1.markerAt(500, 10000));
        assertThat(deserialized.startMarker(inst2)).isEqualTo(inst2.markerAt(99999, 0));
        assertThat(deserialized.startMarker(inst3)).isEqualTo(inst3.markerAt(10000000, 120301312));
    }

    @Test
    public void testPerRangeJdkSerialization()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
        CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
        CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");

        PerRangeCommitLogMarkers.PerRangeBuilder builder = CommitLogMarkers.perRangeBuilder();
        builder.add(TokenRange.closed(BigInteger.ZERO, BigInteger.valueOf(5000)), inst1.markerAt(500, 10000));
        builder.add(TokenRange.closed(BigInteger.valueOf(5000), BigInteger.valueOf(10000)), inst1.markerAt(600, 20000));
        builder.add(TokenRange.closed(BigInteger.valueOf(10000), BigInteger.valueOf(15000)), inst2.markerAt(99999, 0));
        builder.add(TokenRange.closed(BigInteger.valueOf(15000), BigInteger.valueOf(20000)), inst2.markerAt(2000, 500));
        builder.add(TokenRange.closed(BigInteger.valueOf(20000), BigInteger.valueOf(25000)), inst3.markerAt(0, 0));
        builder.add(TokenRange.closed(BigInteger.valueOf(25000), BigInteger.valueOf(30000)), inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE));
        builder.add(TokenRange.closed(BigInteger.valueOf(20000), BigInteger.valueOf(30000)), inst3.markerAt(500, 500));
        PerRangeCommitLogMarkers markers = builder.build();

        byte[] ar = serialize(markers);
        CommitLogMarkers deserialized = deserialize(ar, PerRangeCommitLogMarkers.class);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized).isEqualTo(markers);
        assertThat(deserialized.startMarker(inst1)).isEqualTo(inst1.markerAt(500, 10000));
        assertThat(deserialized.startMarker(inst2)).isEqualTo(inst2.markerAt(2000, 500));
        assertThat(deserialized.startMarker(inst3)).isEqualTo(inst3.zeroMarker());
    }

    public static <T> T deserialize(byte[] ar, Class<T> cType)
    {
        ObjectInputStream in;
        try
        {
            in = new ObjectInputStream(new ByteArrayInputStream(ar));
            return cType.cast(in.readObject());
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(Serializable serializable)
    {
        try
        {
            ByteArrayOutputStream arOut = new ByteArrayOutputStream(512);
            try (ObjectOutputStream out = new ObjectOutputStream(arOut))
            {
                out.writeObject(serializable);
            }
            return arOut.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
