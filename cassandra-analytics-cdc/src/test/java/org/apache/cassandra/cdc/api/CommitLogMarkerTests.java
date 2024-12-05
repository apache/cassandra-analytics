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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommitLogMarkerTests
{
    @Test
    public void testEmpty()
    {
        CassandraInstance inst = new CassandraInstance("0", "local1-i1", "DC1");
        Marker marker = CommitLogMarkers.EMPTY.startMarker(inst);
        assertEquals(0, marker.segmentId);
        assertEquals(0, marker.position);
        assertFalse(CommitLogMarkers.EMPTY.canIgnore(inst.zeroMarker(), BigInteger.ZERO));
        assertFalse(CommitLogMarkers.EMPTY.canIgnore(inst.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), Partitioner.Murmur3Partitioner.maxToken()));
        assertFalse(CommitLogMarkers.EMPTY.canIgnore(inst.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), Partitioner.Murmur3Partitioner.minToken()));
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

        assertEquals(inst1.markerAt(500, 10000), markers.startMarker(inst1));
        assertEquals(inst2.markerAt(99999, 0), markers.startMarker(inst2));
        assertEquals(inst3.markerAt(10000000, 120301312), markers.startMarker(inst3));

        assertFalse(markers.canIgnore(inst1.zeroMarker(), BigInteger.ZERO));
        assertFalse(markers.canIgnore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), BigInteger.ZERO));
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
        assertEquals(inst1.markerAt(500, 10000), markers.startMarker(inst1));
        assertEquals(inst2.markerAt(2000, 500), markers.startMarker(inst2));
        assertEquals(inst3.zeroMarker(), markers.startMarker(inst3));

        // verify CommitLog positions we can/can't ignore on instance 1
        assertTrue(markers.canIgnore(inst1.markerAt(400, 0), BigInteger.ZERO));
        assertTrue(markers.canIgnore(inst1.markerAt(400, 0), BigInteger.ONE));
        assertTrue(markers.canIgnore(inst1.markerAt(500, 0), BigInteger.ONE));
        assertFalse(markers.canIgnore(inst1.markerAt(500, 10000), BigInteger.ZERO));
        assertFalse(markers.canIgnore(inst1.markerAt(500, 10000), BigInteger.ONE));
        assertFalse(markers.canIgnore(inst1.markerAt(500, 10001), BigInteger.ONE));
        assertFalse(markers.canIgnore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE), BigInteger.ONE));

        // verify CommitLog positions we can/can't ignore on instance 2
        assertFalse(markers.canIgnore(inst2.zeroMarker(), BigInteger.ZERO));
        assertFalse(markers.canIgnore(inst2.zeroMarker(), BigInteger.valueOf(7000)));
        assertTrue(markers.canIgnore(inst2.zeroMarker(), BigInteger.valueOf(11000)));
        assertTrue(markers.canIgnore(inst2.markerAt(99998, Integer.MAX_VALUE), BigInteger.valueOf(11000)));
        assertFalse(markers.canIgnore(inst2.markerAt(99999, 0), BigInteger.valueOf(11000)));

        // verify CommitLog positions we can/can't ignore on instance 3
        assertFalse(markers.canIgnore(inst3.zeroMarker(), BigInteger.ZERO));
        assertFalse(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(20000)));
        assertTrue(markers.canIgnore(inst3.zeroMarker(), BigInteger.valueOf(20000)));
        assertTrue(markers.canIgnore(inst3.markerAt(500, 499), BigInteger.valueOf(20000)));
        assertFalse(markers.canIgnore(inst3.markerAt(500, 500), BigInteger.valueOf(20000)));
        assertTrue(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(25000)));
        assertTrue(markers.canIgnore(inst3.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1), BigInteger.valueOf(30000)));
    }

    @Test
    public void testIsBefore()
    {
        CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");

        assertFalse(inst1.zeroMarker().isBefore(inst1.zeroMarker()));

        assertTrue(inst1.zeroMarker().isBefore(inst1.markerAt(0, 1)));
        assertTrue(inst1.zeroMarker().isBefore(inst1.markerAt(1, 1)));
        assertTrue(inst1.markerAt(1, 0).isBefore(inst1.markerAt(1, 1)));
        assertTrue(inst1.markerAt(10000, Integer.MAX_VALUE).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE)));
        assertTrue(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE - 1).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE)));

        assertFalse(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE).isBefore(inst1.zeroMarker()));
        assertFalse(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE).isBefore(inst1.markerAt(Long.MAX_VALUE, Integer.MAX_VALUE)));
        assertFalse(inst1.markerAt(10000, 5001).isBefore(inst1.markerAt(10000, 5000)));
        assertFalse(inst1.markerAt(10001, 5000).isBefore(inst1.markerAt(10000, 5000)));
    }

    @Test
    public void testIsBeforeException()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
            CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
            assertTrue(inst1.zeroMarker().isBefore(inst2.zeroMarker()));
        });
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
        assertNotNull(deserialized);
        assertEquals(markers, deserialized);
        assertEquals(inst1.markerAt(500, 10000), deserialized.startMarker(inst1));
        assertEquals(inst2.markerAt(99999, 0), deserialized.startMarker(inst2));
        assertEquals(inst3.markerAt(10000000, 120301312), deserialized.startMarker(inst3));
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
        assertNotNull(deserialized);
        assertEquals(markers, deserialized);
        assertEquals(inst1.markerAt(500, 10000), deserialized.startMarker(inst1));
        assertEquals(inst2.markerAt(2000, 500), deserialized.startMarker(inst2));
        assertEquals(inst3.zeroMarker(), deserialized.startMarker(inst3));
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
