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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/*
 * An enum that describes all possible Cassandra versions that can potentially be supported, even if the bridge is not yet implemented.
 * Customers of this library looking to implement additional bridges or replace existing ones with proprietary implementations
 * should inject/replace bridge implementation JARs embedded into this library's resources and replace this class with an identical one,
 * but with implementedVersions() and supportedVersions() modified accordingly.
 *
 * NOTE: The following values need to stay in sync with:
 * - build.gradle:
 *   - ext.cassandraVersionEnumMap = ["4.0": "FOURZERO", "4.1": "FOURONE", "5.0": "FIVEZERO"]
 *   - ext.cassandraFullVersionMap = ["4.0": "4.0.17", "4.1": "4.1.4", "5.0": "5.0.5"]
 * - build-dtest-jars.sh:
 *   - CANDIDATE_BRANCHES=(
 *      "cassandra-4.0:cassandra-4.0.17"
 *      "cassandra-4.1:99d9faeef57c9cf5240d11eac9db5b283e45a4f9"
 *      "cassandra-5.0:cassandra-5.0.5"
 */
public enum CassandraVersion
{
    THREEZERO(30, "3.0", "three-zero", new String[]{"big"},
              new String[]{
                  // Cassandra 3.x native sstable versions
                  "big-ma",
                  "big-mb",
                  "big-mc",
                  "big-md",
                  "big-me",
                  "big-mf"
              }),
    FOURZERO(40, "4.0", "four-zero", new String[]{"big"},
             new String[]{
                 // Cassandra 4.0 native sstable versions
                 "big-na",
                 "big-nb",
             }),
    FOURONE(41, "4.1", "four-zero", new String[]{"big"},
            new String[]{
                // Cassandra 4.1 did not introduce new native SSTable versions
            }),
    FIVEZERO(50, "5.0", "five-zero", new String[]{"big", "bti"},
             new String[]{
                 // Cassandra 5.0 native sstable versions
                 "big-oa",
                 "bti-da",
             });

    private final int number;
    private final String name;
    private final String jarBaseName;  // Must match shadowJar.archiveFileName from Gradle configuration (without extension)
    private final Set<String> sstableFormats;
    private final List<String> nativeSStableVersions;


    CassandraVersion(int number, String name, String jarBaseName, String[] sstableFormats, String[] nativeSStableVersions)
    {
        this.number = number;
        this.name = name;
        this.jarBaseName = jarBaseName;
        this.sstableFormats = new HashSet<>(Arrays.asList(sstableFormats));
        this.nativeSStableVersions = List.of(nativeSStableVersions);
    }

    public int versionNumber()
    {
        return number;
    }

    public String versionName()
    {
        return name;
    }

    public String jarBaseName()
    {
        return jarBaseName;
    }

    /**
     * Get the set of SSTable formats supported by this Cassandra version.
     *
     * @return Set of supported SSTable format strings
     */
    public Set<String> sstableFormats()
    {
        return sstableFormats;
    }

    /**
     * Get the list of native SSTable version strings for this Cassandra version.
     *
     * @return List of native SSTable version strings
     */
    public List<String> getNativeSStableVersions()
    {
        return nativeSStableVersions;
    }

    /**
     * Get the set of SSTable version strings that this Cassandra version can read.
     * This includes:
     * - Native versions for this Cassandra version
     * - All SSTable versions from the previous major version (including all minor versions)
     * For example, Cassandra 5.0 can read:
     * - 5.0 native versions (big-oa, bti-da)
     * - 4.0 versions (big-na, big-nb)
     * - 4.1 versions (if any)
     * But NOT 3.0 versions
     *
     * @return Set of full SSTable version strings that can be read
     */
    public Set<String> getSupportedSStableVersionsForRead()
    {
        Set<String> readableVersions = new HashSet<>(this.nativeSStableVersions);

        int previousMajor = getPreviousMajorVersion();

        // Add all SSTable versions from the previous major version and its minors
        // E.g., C* 5.0 (version 50) can read C* 4.0 (40) and C* 4.1 (41) SSTables, but not C* 3.x (30)
        for (CassandraVersion version : CassandraVersion.values())
        {
            // Include versions from the previous major version family (e.g., 40-49 for C* 5.0)
            if (version.versionNumber() >= previousMajor && version.versionNumber() < this.number)
            {
                readableVersions.addAll(version.nativeSStableVersions);
            }
        }

        return Collections.unmodifiableSet(readableVersions);
    }

    /**
     * Get the previous major version number for this Cassandra version.
     * Calculates dynamically using: (majorVersion - 1) * 10
     * For example:
     * - C5.0 (50) returns 40 (C4.x)
     * - C4.1 (41) returns 30 (C3.x)
     * - C4.0 (40) returns 30 (C3.x)
     * - C3.0 (30) returns 20 (C2.x - which doesn't exist)
     * - C10.0 (100) returns 90 (C9.x)
     *
     * @return previous major version number
     */
    @VisibleForTesting
    int getPreviousMajorVersion()
    {
        // Get major version: 50 -> 5, 41 -> 4, 40 -> 4, 30 -> 3
        int majorVersion = this.number / 10;

        // Calculate previous major version: (majorVersion - 1) * 10
        // E.g., 5 -> 40, 4 -> 30, 3 -> 20
        return (majorVersion - 1) * 10;
    }

    private static final String configuredSSTableFormat;
    private static final CassandraVersion[] implementedVersions;
    private static final String[] supportedVersions;

    static
    {
        configuredSSTableFormat = System.getProperty("cassandra.analytics.bridges.sstable_format", "big");

        // NOTE: These default enum names must stay in sync with cassandraVersionEnumMap in build.gradle.
        // FOURONE is intentionally excluded from local-dev defaults to keep iteration fast;
        // CI covers 4.1 via explicit CASSANDRA_VERSION env var or per-version Gradle tasks (e.g. testCassandra41).
        String providedVersionsOrDefault = System.getProperty("cassandra.analytics.bridges.implemented_versions",
                                                              String.join(",", FOURZERO.name(), FIVEZERO.name()));
        implementedVersions = Arrays.stream(providedVersionsOrDefault.split(","))
                                    .map(CassandraVersion::valueOf)
                                    .filter(v -> v.sstableFormats().contains(configuredSSTableFormat))
                                    .toArray(CassandraVersion[]::new);

        // NOTE: These default versions must stay in sync with cassandraFullVersionMap in build.gradle.
        String providedSupportedVersionsOrDefault = System.getProperty("cassandra.analytics.bridges.supported_versions",
                                                                       "cassandra-4.0.17,cassandra-5.0.5");
        supportedVersions = Arrays.stream(providedSupportedVersionsOrDefault.split(","))
                                  .filter(version -> CassandraVersion.fromVersion(version)
                                                                     .filter(v -> v.sstableFormats().contains(configuredSSTableFormat))
                                                                     .isPresent())
                                  .toArray(String[]::new);

        Preconditions.checkArgument(implementedVersions.length > 0 && supportedVersions.length > 0,
                                    "No versions available");
    }

    public static String configuredSSTableFormat()
    {
        return configuredSSTableFormat;
    }

    public static Optional<CassandraVersion> fromVersion(String cassandraVersion)
    {
        CassandraVersionFeatures features = CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(cassandraVersion);
        return Arrays.stream(CassandraVersion.values())
                     .filter(value -> value.versionNumber() == features.getMajorVersion())
                     .findAny();
    }

    /**
     * Find the Cassandra version that originally writes SSTables with this version string.
     * Returns the native Cassandra version that introduced this SSTable version.
     *
     * @param sstableVersion full version string including format (e.g., "big-na", "bti-da")
     * @return Optional containing the CassandraVersion that natively writes this format,
     *         or Optional.empty() if:
     *         <ul>
     *           <li>The version string is null</li>
     *           <li>The version string is unrecognized (not in any enum's nativeSStableVersions)</li>
     *           <li>The version format is invalid or doesn't match expected pattern</li>
     *         </ul>
     */
    public static Optional<CassandraVersion> fromSSTableVersion(String sstableVersion)
    {
        if (sstableVersion == null)
        {
            return Optional.empty();
        }

        for (CassandraVersion version : CassandraVersion.values())
        {
            if (version.nativeSStableVersions.contains(sstableVersion))
            {
                return Optional.of(version);
            }
        }

        return Optional.empty();
    }

    public static CassandraVersion[] implementedVersions()
    {
        return implementedVersions;
    }

    @VisibleForTesting
    public static String[] supportedVersions()
    {
        return supportedVersions;
    }
}
