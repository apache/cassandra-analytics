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

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CassandraVersion SSTable version methods
 */
public class CassandraVersionTest
{
    @ParameterizedTest
    @CsvSource({
        "big-ma, THREEZERO", "big-mb, THREEZERO", "big-mc, THREEZERO",
        "big-md, THREEZERO", "big-me, THREEZERO", "big-mf, THREEZERO",
        "big-na, FOURZERO", "big-nb, FOURZERO",
        "big-oa, FIVEZERO", "bti-da, FIVEZERO"
    })
    void testFromSSTableVersionNativeVersions(String ssTableVersion, String expectedVersion)
    {
        Optional<CassandraVersion> result = CassandraVersion.fromSSTableVersion(ssTableVersion);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(CassandraVersion.valueOf(expectedVersion));
    }

    @Test
    void testFromSSTableVersionReturnsEmpty()
    {
        Optional<CassandraVersion> result = CassandraVersion.fromSSTableVersion("unknown-xx");
        assertThat(result).isEmpty();
    }

    @Test
    void testGetSupportedSStableVersionsForReadFourZero()
    {
        Set<String> supported = CassandraVersion.FOURZERO.getSupportedSStableVersionsForRead();

        // C* 4.0 can read its own versions
        assertThat(supported).contains("big-na", "big-nb");

        // C* 4.0 can read C* 3.0 versions (previous major version family)
        assertThat(supported).contains("big-ma", "big-mb", "big-mc", "big-md", "big-me", "big-mf");

        // C* 4.0 cannot read C* 5.0 versions
        assertThat(supported).doesNotContain("big-oa", "bti-da");
    }

    @Test
    void testGetSupportedSStableVersionsForReadFiveZero()
    {
        Set<String> supported = CassandraVersion.FIVEZERO.getSupportedSStableVersionsForRead();

        // C* 5.0 can read its own versions
        assertThat(supported).contains("big-oa", "bti-da");

        // C* 5.0 can read C* 4.0 and 4.1 versions (previous major version family)
        assertThat(supported).contains("big-na", "big-nb");

        // C* 5.0 cannot read C* 3.0 versions (not in previous major version family)
        assertThat(supported).doesNotContain("big-ma", "big-mb", "big-mc", "big-md", "big-me", "big-mf");
    }

    @Test
    void testGetSupportedSStableVersionsForReadThreeZero()
    {
        Set<String> supported = CassandraVersion.THREEZERO.getSupportedSStableVersionsForRead();

        // C* 3.0 can read its own versions
        assertThat(supported).contains("big-ma", "big-mb", "big-mc", "big-md", "big-me", "big-mf");

        // C* 3.0 cannot read C* 4.0+ versions
        assertThat(supported).doesNotContain("big-na", "big-nb", "big-oa", "bti-da");

        // C* 3.0's previous major version (2.x) is not defined, so only its own versions are readable
        assertThat(supported).hasSize(6);  // Only the 6 native 3.0 versions
    }

    @Test
    void testGetSupportedSStableVersionsForReadFourOne()
    {
        Set<String> supported = CassandraVersion.FOURONE.getSupportedSStableVersionsForRead();

        // C* 4.1 has no native SSTable versions of its own, but can read C* 4.0 and C* 3.0 versions
        // C* 4.1 can read C* 4.0 versions (same major version family)
        assertThat(supported).contains("big-na", "big-nb");

        // C* 4.1 can read C* 3.0 versions (previous major version family)
        assertThat(supported).contains("big-ma", "big-mb", "big-mc", "big-md", "big-me", "big-mf");

        // C* 4.1 cannot read C* 5.0 versions
        assertThat(supported).doesNotContain("big-oa", "bti-da");
    }

    @Test
    void testGetNativeSStableVersionsFourZero()
    {
        List<String> nativeVersions = CassandraVersion.FOURZERO.getNativeSStableVersions();
        assertThat(nativeVersions).containsExactlyInAnyOrder("big-na", "big-nb");
    }

    @Test
    void testGetNativeSStableVersionsFiveZero()
    {
        List<String> nativeVersions = CassandraVersion.FIVEZERO.getNativeSStableVersions();
        assertThat(nativeVersions).containsExactlyInAnyOrder("big-oa", "bti-da");
    }

    @Test
    void testGetNativeSStableVersionsFourOneEmpty()
    {
        // C* 4.1 did not introduce new native SSTable versions
        List<String> nativeVersions = CassandraVersion.FOURONE.getNativeSStableVersions();
        assertThat(nativeVersions).isEmpty();
    }

    @Test
    void testConfiguredSSTableFormatDefault()
    {
        // Assuming no system property set, should return "big"
        String format = CassandraVersion.configuredSSTableFormat();
        assertThat(format).isNotNull();
        assertThat(format).isIn("big", "bti"); // Could be either depending on env
    }

    @Test
    void testSStableFormatsFourZero()
    {
        Set<String> formats = CassandraVersion.FOURZERO.sstableFormats();
        assertThat(formats).containsExactly("big");
    }

    @Test
    void testSStableFormatsFiveZero()
    {
        Set<String> formats = CassandraVersion.FIVEZERO.sstableFormats();
        assertThat(formats).containsExactly("big", "bti");
    }
}
