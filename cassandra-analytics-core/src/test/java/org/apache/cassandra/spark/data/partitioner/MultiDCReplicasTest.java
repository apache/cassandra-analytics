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

package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link MultiDCReplicas} class
 */
public class MultiDCReplicasTest
{
    @Mock
    private SSTablesSupplier dc1Supplier;

    @Mock
    private SSTablesSupplier dc2Supplier;

    @Mock
    private SSTablesSupplier.ReaderOpener<TestSparkSSTableReader> readerOpener;

    private MultiDCReplicas multiDCReplicas;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testConstructorWithValidMap()
    {
        Map<String, SSTablesSupplier> replicasMap = new HashMap<>();
        replicasMap.put("dc1", dc1Supplier);
        replicasMap.put("dc2", dc2Supplier);

        multiDCReplicas = new MultiDCReplicas(replicasMap);

        assertThat(multiDCReplicas).isNotNull();
    }

    @Test
    void testConstructorWithNullMap()
    {
        assertThatThrownBy(() -> new MultiDCReplicas(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("replicasPerDC cannot be null or empty");
    }

    @Test
    void testConstructorWithEmptyMap()
    {
        assertThatThrownBy(() -> new MultiDCReplicas(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("replicasPerDC cannot be null or empty");
    }

    @Test
    void testOpenAllWithMultipleDCs()
    {
        Map<String, SSTablesSupplier> replicasMap = Map.of("dc1", dc1Supplier,
                                                           "dc2", dc2Supplier);

        TestSparkSSTableReader reader1 = new TestSparkSSTableReader(BigInteger.ONE, BigInteger.TEN);
        TestSparkSSTableReader reader2 = new TestSparkSSTableReader(BigInteger.valueOf(11), BigInteger.valueOf(20));
        TestSparkSSTableReader reader3 = new TestSparkSSTableReader(BigInteger.valueOf(21), BigInteger.valueOf(30));

        Set<TestSparkSSTableReader> dc1Readers = Set.of(reader1, reader2);
        Set<TestSparkSSTableReader> dc2Readers = Set.of(reader3);

        when(dc1Supplier.openAll(readerOpener)).thenReturn(dc1Readers);
        when(dc2Supplier.openAll(readerOpener)).thenReturn(dc2Readers);

        multiDCReplicas = new MultiDCReplicas(replicasMap);

        Set<TestSparkSSTableReader> result = multiDCReplicas.openAll(readerOpener);

        assertThat(result).hasSize(3);
        assertThat(result).containsExactlyInAnyOrder(reader1, reader2, reader3);
    }

    @Test
    void testOpenAllWithSingleDC()
    {
        Map<String, SSTablesSupplier> replicasMap = new HashMap<>();
        replicasMap.put("dc1", dc1Supplier);

        TestSparkSSTableReader reader1 = new TestSparkSSTableReader(BigInteger.ONE, BigInteger.TEN);
        TestSparkSSTableReader reader2 = new TestSparkSSTableReader(BigInteger.valueOf(11), BigInteger.valueOf(20));

        Set<TestSparkSSTableReader> dc1Readers = Set.of(reader1, reader2);

        when(dc1Supplier.openAll(readerOpener)).thenReturn(dc1Readers);

        multiDCReplicas = new MultiDCReplicas(replicasMap);

        Set<TestSparkSSTableReader> result = multiDCReplicas.openAll(readerOpener);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder(reader1, reader2);
    }

    @Test
    void testOpenAllWithEmptySupplierResults()
    {
        // Arrange
        Map<String, SSTablesSupplier> replicasMap = new HashMap<>();
        replicasMap.put("dc1", dc1Supplier);
        replicasMap.put("dc2", dc2Supplier);

        when(dc1Supplier.openAll(readerOpener)).thenReturn(new HashSet<>());
        when(dc2Supplier.openAll(readerOpener)).thenReturn(new HashSet<>());

        multiDCReplicas = new MultiDCReplicas(replicasMap);

        Set<TestSparkSSTableReader> result = multiDCReplicas.openAll(readerOpener);

        assertThat(result).isNotNull();
        assertThat(result).isEmpty();
    }

    @Test
    void testOpenAllWithDuplicateReaders()
    {
        Map<String, SSTablesSupplier> replicasMap = new HashMap<>();
        replicasMap.put("dc1", dc1Supplier);
        replicasMap.put("dc2", dc2Supplier);

        TestSparkSSTableReader reader1 = new TestSparkSSTableReader(BigInteger.ONE, BigInteger.TEN);
        TestSparkSSTableReader reader2 = new TestSparkSSTableReader(BigInteger.valueOf(11), BigInteger.valueOf(20));

        Set<TestSparkSSTableReader> dc1Readers = Set.of(reader1, reader2);
        Set<TestSparkSSTableReader> dc2Readers = Set.of(reader1, reader2);

        when(dc1Supplier.openAll(readerOpener)).thenReturn(dc1Readers);
        when(dc2Supplier.openAll(readerOpener)).thenReturn(dc2Readers);

        multiDCReplicas = new MultiDCReplicas(replicasMap);

        Set<TestSparkSSTableReader> result = multiDCReplicas.openAll(readerOpener);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactlyInAnyOrder(reader1, reader2);
    }

    /**
     * Test implementation of SparkSSTableReader for testing purposes
     */
    private static class TestSparkSSTableReader implements SparkSSTableReader
    {
        private final BigInteger firstToken;
        private final BigInteger lastToken;

        TestSparkSSTableReader(BigInteger firstToken, BigInteger lastToken)
        {
            this.firstToken = firstToken;
            this.lastToken = lastToken;
        }

        @Override
        public BigInteger firstToken()
        {
            return firstToken;
        }

        @Override
        public BigInteger lastToken()
        {
            return lastToken;
        }

        public boolean ignore()
        {
            return false;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof TestSparkSSTableReader))
            {
                return false;
            }
            TestSparkSSTableReader that = (TestSparkSSTableReader) obj;
            return firstToken.equals(that.firstToken) && lastToken.equals(that.lastToken);
        }

        @Override
        public int hashCode()
        {
            return firstToken.hashCode() + lastToken.hashCode();
        }
    }
}
