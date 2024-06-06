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

import java.math.BigInteger;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OverlapTests
{
    @Test
    public void testBasic()
    {
        SparkSSTableReader reader = reader(100L, 200L);
        assertFalse(SparkSSTableReader.overlaps(reader, range(-10, -5)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(0, 0)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(-1, -1)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(50, 55)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(95, 100)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(100, 100)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(100, 105)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(100, 150)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(160, 200)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(200, 200)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(200, 205)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(201, 205)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(500, 550)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(50, 250)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                             Partitioner.Murmur3Partitioner.maxToken())));
    }

    @Test
    public void testMurmur3()
    {
        SparkSSTableReader murmur3Reader = reader(Partitioner.Murmur3Partitioner.minToken(),
                                                  Partitioner.Murmur3Partitioner.maxToken());
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(-10, -5)));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(0, 0)));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(300, 400)));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                    Partitioner.Murmur3Partitioner.minToken())));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                    Partitioner.Murmur3Partitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(Partitioner.Murmur3Partitioner.maxToken(),
                                                                    Partitioner.Murmur3Partitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(murmur3Reader, range(Partitioner.Murmur3Partitioner.minToken().add(BigInteger.ONE),
                                                                    Partitioner.Murmur3Partitioner.maxToken().subtract(BigInteger.ONE))));
    }

    @Test
    public void testRandom()
    {
        SparkSSTableReader randomReader = reader(Partitioner.RandomPartitioner.minToken(),
                                                 Partitioner.RandomPartitioner.maxToken());
        assertFalse(SparkSSTableReader.overlaps(randomReader, range(-10, -5)));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(0, 0)));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(5000, 6000)));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(Partitioner.RandomPartitioner.minToken(),
                                                                   Partitioner.RandomPartitioner.minToken())));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(Partitioner.RandomPartitioner.minToken(),
                                                                   Partitioner.RandomPartitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(Partitioner.RandomPartitioner.maxToken(),
                                                                   Partitioner.RandomPartitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(randomReader, range(Partitioner.RandomPartitioner.minToken().add(BigInteger.ONE),
                                                                   Partitioner.RandomPartitioner.maxToken().subtract(BigInteger.ONE))));
    }

    @Test
    public void testMinEdge()
    {
        SparkSSTableReader minReader = reader(Partitioner.Murmur3Partitioner.minToken(),
                                              BigInteger.valueOf(-6661324248839560306L));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                Partitioner.Murmur3Partitioner.minToken())));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                BigInteger.valueOf(-8198552921648689608L))));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(Partitioner.Murmur3Partitioner.minToken().subtract(BigInteger.ONE),
                                                                BigInteger.valueOf(-8198552921648689608L))));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(Partitioner.Murmur3Partitioner.minToken().subtract(BigInteger.TEN),
                                                                BigInteger.valueOf(-7173733806442603407L))));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(-7173733806442603407L, -6148914691236517207L)));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(-6661324248839560307L, -6661324248839560306L)));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(-6661324248839560306L, -6661324248839560306L)));
        assertTrue(SparkSSTableReader.overlaps(minReader, range(-6661324248839560307L, -6148914691236517206L)));
        assertFalse(SparkSSTableReader.overlaps(minReader, range(-6661324248839560305L, -6661324248839560305L)));
        assertFalse(SparkSSTableReader.overlaps(minReader, range(-4611686018427387904L, -2562047788015215503L)));
        assertFalse(SparkSSTableReader.overlaps(minReader, range(0L, 0L)));
        assertFalse(SparkSSTableReader.overlaps(minReader, range(512409557603043100L, 8710962479251732707L)));
        assertFalse(SparkSSTableReader.overlaps(minReader, range(Partitioner.Murmur3Partitioner.maxToken(),
                                                                 Partitioner.Murmur3Partitioner.maxToken())));
    }

    @Test
    public void testMaxEdge()
    {
        SparkSSTableReader maxReader = reader(BigInteger.valueOf(2049638230412172401L),
                                              Partitioner.Murmur3Partitioner.maxToken());
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                 Partitioner.Murmur3Partitioner.minToken())));
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                                 Partitioner.Murmur3Partitioner.minToken().add(BigInteger.TEN))));
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(-3074457345618258603L, -1537228672809129302L)));
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(-512409557603043101L, 0L)));
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(-512409557603043101L, 1024819115206086200L)));
        assertFalse(SparkSSTableReader.overlaps(maxReader, range(512409557603043100L, 1537228672809129301L)));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(2049638230412172400L, 2049638230412172401L)));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(2049638230412172401L, 2049638230412172401L)));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(2049638230412172402L, 2049638230412172402L)));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(2049638230412172401L, 5636505133633474104L)));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(BigInteger.valueOf(2049638230412172401L),
                                                                Partitioner.Murmur3Partitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(BigInteger.valueOf(6661324248839560305L),
                                                                Partitioner.Murmur3Partitioner.maxToken())));
        assertTrue(SparkSSTableReader.overlaps(maxReader, range(Partitioner.Murmur3Partitioner.maxToken(),
                                                                Partitioner.Murmur3Partitioner.maxToken())));
    }

    @Test
    public void testZeroWrap()
    {
        SparkSSTableReader reader = reader(-1537228672809129302L, 1537228672809129301L);
        assertFalse(SparkSSTableReader.overlaps(reader, range(Partitioner.Murmur3Partitioner.minToken(),
                                                              Partitioner.Murmur3Partitioner.minToken())));
        assertFalse(SparkSSTableReader.overlaps(reader, range(-5636505133633474105L, -2562047788015215503L)));
        assertFalse(SparkSSTableReader.overlaps(reader, range(-1537228672809129303L, -1537228672809129303L)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(-1537228672809129302L, -1537228672809129302L)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(-1537228672809129301L, -1537228672809129301L)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(-1537228672809129302L, 0)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(0, 0)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(0, 1024819115206086200L)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(0, 1537228672809129301L)));
        assertTrue(SparkSSTableReader.overlaps(reader, range(1537228672809129301L, 1537228672809129301L)));
    }

    private static TokenRange range(long start, long end)
    {
        return range(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    private static TokenRange range(BigInteger start, BigInteger end)
    {
        return TokenRange.closed(start, end);
    }

    private static SparkSSTableReader reader(long start, long end)
    {
        return reader(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    private static SparkSSTableReader reader(BigInteger start, BigInteger end)
    {
        Preconditions.checkArgument(start.compareTo(end) <= 0, "Start token must be less than end token");
        return new SparkSSTableReader()
        {
            public BigInteger firstToken()
            {
                return start;
            }

            public BigInteger lastToken()
            {
                return end;
            }

            public boolean ignore()
            {
                return false;
            }
        };
    }
}
