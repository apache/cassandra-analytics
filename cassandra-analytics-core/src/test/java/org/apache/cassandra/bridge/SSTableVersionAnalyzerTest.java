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
    // --- determineBridgeVersionForWrite: picks the LOWEST mutually-compatible version ---

    static Stream<Arguments> writeLowestVersionCases()
    {
        return Stream.of(
        Arguments.of(Collections.singleton("big-oa"), "big", CassandraVersion.FIVEZERO),
        Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-nb")), "big", CassandraVersion.FOURZERO),
        // mixed 4.0 + 5.0 sstables: lowest that every node can import is 4.0
        Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-oa")), "big", CassandraVersion.FOURZERO),
        Arguments.of(Collections.singleton("bti-da"), "bti", CassandraVersion.FIVEZERO)
        );
    }

    @ParameterizedTest
    @MethodSource("writeLowestVersionCases")
    void testDetermineBridgeVersionForWritePicksLowest(Set<String> versions, String format, CassandraVersion expected)
    {
        assertThat(SSTableVersionAnalyzer.determineBridgeVersionForWrite(versions, format)).isEqualTo(expected);
    }

    @Test
    void testDetermineBridgeVersionForWriteRejectsUnsupportedFormat()
    {
        // lowest version is FOURZERO, which does not support bti
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(
        new HashSet<>(Arrays.asList("big-na", "big-oa")), "bti"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support the requested SSTable format 'bti'")
        .hasMessageContaining("retry with a supported SSTable format")
        .hasMessageContaining("disable_sstable_version_based");

        // Freshly upgraded to C* 5.x with bti configured, but only pre-upgrade big-na SSTables are present:
        // the lowest version present is FOURZERO (4.0 bridge), which cannot write the bti format, so we reject.
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(
        new HashSet<>(Arrays.asList("big-na", "bti-da")), "bti"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support the requested SSTable format 'bti'")
        .hasMessageContaining("retry with a supported SSTable format")
        .hasMessageContaining("disable_sstable_version_based");
    }

    @Test
    void testDetermineBridgeVersionForWriteRejectsIncompatibleVersions()
    {
        // big-mf (3.0) cannot be read by the highest version present (5.0)
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(
        new HashSet<>(Arrays.asList("big-mf", "big-oa")), "big"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not mutually compatible");
    }

    static Stream<Arguments> nullOrEmptyCases()
    {
        return Stream.of(Arguments.of(Collections.emptySet()), Arguments.of((Set<String>) null));
    }

    @ParameterizedTest
    @MethodSource("nullOrEmptyCases")
    void testDetermineBridgeVersionForWriteNullOrEmptyThrows(Set<String> versions)
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(versions, "big"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to retrieve SSTable versions from cluster")
        .hasMessageContaining("disable_sstable_version_based");
    }

    @Test
    void testDetermineBridgeVersionForWriteUnknownVersionThrows()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(Collections.singleton("unknown-xx"), "big"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unknown SSTable version");
    }

    // --- determineBridgeVersionForRead: picks the HIGHEST mutually-compatible version ---

    static Stream<Arguments> readHighestVersionCases()
    {
        return Stream.of(
        Arguments.of(Collections.singleton("big-oa"), CassandraVersion.FIVEZERO),
        Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-nb")), CassandraVersion.FOURZERO),
        Arguments.of(new HashSet<>(Arrays.asList("big-na", "bti-da")), CassandraVersion.FIVEZERO),
        Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-oa")), CassandraVersion.FIVEZERO)
        );
    }

    @ParameterizedTest
    @MethodSource("readHighestVersionCases")
    void testDetermineBridgeVersionForReadPicksHighest(Set<String> versions, CassandraVersion expected)
    {
        assertThat(SSTableVersionAnalyzer.determineBridgeVersionForRead(versions)).isEqualTo(expected);
    }

    @Test
    void testDetermineBridgeVersionForReadRejectsIncompatibleVersions()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForRead(
        new HashSet<>(Arrays.asList("big-mf", "big-oa"))))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not mutually compatible");
    }

    @ParameterizedTest
    @MethodSource("nullOrEmptyCases")
    void testDetermineBridgeVersionForReadNullOrEmptyThrows(Set<String> versions)
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForRead(versions))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to retrieve SSTable versions from cluster")
        .hasMessageContaining("disable_sstable_version_based");
    }

    // --- lowestCompatibleWriteVersionForCoordinatedWrites: coordinated multi-cluster write target ---

    @Test
    void testLowestCompatibleWriteVersionPicksLowestWhenCompatible()
    {
        // 4.0 + 5.0: SSTables written at 4.0 are importable by 5.0, so the lowest (4.0) is chosen
        assertThat(SSTableVersionAnalyzer.lowestCompatibleWriteVersionForCoordinatedWrites(
        Arrays.asList(CassandraVersion.FOURZERO, CassandraVersion.FIVEZERO)))
        .isEqualTo(CassandraVersion.FOURZERO);
    }

    @Test
    void testLowestCompatibleWriteVersionSameVersion()
    {
        assertThat(SSTableVersionAnalyzer.lowestCompatibleWriteVersionForCoordinatedWrites(
        Arrays.asList(CassandraVersion.FIVEZERO, CassandraVersion.FIVEZERO)))
        .isEqualTo(CassandraVersion.FIVEZERO);
    }

    @Test
    void testLowestCompatibleWriteVersionRejectsIncompatibleMajors()
    {
        // 3.0 + 5.0: SSTables written at 3.0 cannot be imported by 5.0 -> reject
        assertThatThrownBy(() -> SSTableVersionAnalyzer.lowestCompatibleWriteVersionForCoordinatedWrites(
        Arrays.asList(CassandraVersion.THREEZERO, CassandraVersion.FIVEZERO)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not mutually compatible");
    }

    @Test
    void testLowestCompatibleWriteVersionNullOrEmptyThrows()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.lowestCompatibleWriteVersionForCoordinatedWrites(Collections.emptyList()))
        .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> SSTableVersionAnalyzer.lowestCompatibleWriteVersionForCoordinatedWrites(null))
        .isInstanceOf(IllegalStateException.class);
    }
}
