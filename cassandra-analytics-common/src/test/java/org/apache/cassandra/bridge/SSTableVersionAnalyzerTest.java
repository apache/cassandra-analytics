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

package org.apache.cassandra.bridge;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SSTableVersionAnalyzer
 */
public class SSTableVersionAnalyzerTest
{
    // --- determineBridgeVersionForWrite success cases (parameterized) ---

    static Stream<Arguments> writeFallbackDisabledSuccessCases()
    {
        return Stream.of(
            Arguments.of(Collections.singleton("big-oa"), "big", "5.0.0", CassandraVersion.FIVEZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-nb")), "big", "4.0.0", CassandraVersion.FOURZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-oa")), "big", "5.0.0", CassandraVersion.FIVEZERO)
        );
    }

    @ParameterizedTest
    @MethodSource("writeFallbackDisabledSuccessCases")
    void testDetermineBridgeVersionForWriteFallbackDisabled(Set<String> versions,
                                                            String format,
                                                            String cassandraVersion,
                                                            CassandraVersion expected)
    {
        CassandraVersion result = SSTableVersionAnalyzer.determineBridgeVersionForWrite(
            versions, format, cassandraVersion, false
        );
        assertThat(result).isEqualTo(expected);
    }

    // --- determineBridgeVersionForWrite null/empty exception cases (parameterized) ---

    static Stream<Arguments> writeNullOrEmptyVersionsCases()
    {
        return Stream.of(
            Arguments.of(Collections.emptySet()),
            Arguments.of((Set<String>) null)
        );
    }

    @ParameterizedTest
    @MethodSource("writeNullOrEmptyVersionsCases")
    void testDetermineBridgeVersionForWriteNullOrEmptyThrowsException(Set<String> versions)
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(
            versions, "big", "5.0.0", false
        )).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Unable to retrieve SSTable versions from cluster");
    }

    // --- determineBridgeVersionForWrite standalone tests ---

    @Test
    void testDetermineBridgeVersionForWriteFallbackEnabled()
    {
        CassandraVersion result = SSTableVersionAnalyzer.determineBridgeVersionForWrite(
            null, "big", "5.0.0", true
        );
        assertThat(result).isEqualTo(CassandraVersion.FIVEZERO);
    }

    @Test
    void testDetermineBridgeVersionForWriteUnsupportedFormat()
    {
        Set<String> sstableVersions = Collections.singleton("big-na");
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(
            sstableVersions, "bti", "4.0.0", false
        )).isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Cluster does not support requested SSTable format 'bti'");
    }

    // --- determineBridgeVersionForRead standalone tests ---

    @Test
    void testDetermineBridgeVersionForReadFallbackDisabled()
    {
        Set<String> sstableVersions = Collections.singleton("big-oa");
        CassandraVersion result = SSTableVersionAnalyzer.determineBridgeVersionForRead(
            sstableVersions, "5.0.0", false
        );
        assertThat(result).isEqualTo(CassandraVersion.FIVEZERO);
    }

    @Test
    void testDetermineBridgeVersionForReadFallbackEnabled()
    {
        CassandraVersion result = SSTableVersionAnalyzer.determineBridgeVersionForRead(
            null, "4.0.0", true
        );
        assertThat(result).isEqualTo(CassandraVersion.FOURZERO);
    }

    @Test
    void testDetermineBridgeVersionForReadEmptyVersionsThrowsException()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForRead(
            Collections.emptySet(), "5.0.0", false
        )).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Unable to retrieve SSTable versions from cluster");
    }

    // --- findHighestSSTableVersion success cases (parameterized) ---

    static Stream<Arguments> findHighestSuccessCases()
    {
        return Stream.of(
            Arguments.of(Collections.singleton("big-na"), CassandraVersion.FOURZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-nb")), CassandraVersion.FOURZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-oa")), CassandraVersion.FIVEZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-oa", "bti-da")), CassandraVersion.FIVEZERO)
        );
    }

    @ParameterizedTest
    @MethodSource("findHighestSuccessCases")
    void testFindHighestSSTableVersion(Set<String> versions, CassandraVersion expectedCassandraVersion)
    {
        String result = SSTableVersionAnalyzer.findHighestSSTableVersion(versions);
        assertThat(CassandraVersion.fromSSTableVersion(result)).hasValue(expectedCassandraVersion);
    }

    // --- findHighestSSTableVersion null/empty exception cases (parameterized) ---

    static Stream<Arguments> findHighestNullOrEmptyCases()
    {
        return Stream.of(
            Arguments.of(Collections.emptySet()),
            Arguments.of((Set<String>) null)
        );
    }

    @ParameterizedTest
    @MethodSource("findHighestNullOrEmptyCases")
    void testFindHighestSSTableVersionNullOrEmptyThrowsException(Set<String> versions)
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.findHighestSSTableVersion(versions))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("SSTable versions set cannot be empty");
    }

    // --- findHighestSSTableVersion standalone test ---

    @Test
    void testFindHighestSSTableVersionUnknownVersionThrowsException()
    {
        Set<String> versions = new HashSet<>(Arrays.asList("unknown-xx", "unknown-yy"));
        assertThatThrownBy(() -> SSTableVersionAnalyzer.findHighestSSTableVersion(versions))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Unknown SSTable version:")
            .hasMessageContaining("disable_sstable_version_based=true");
    }
}
