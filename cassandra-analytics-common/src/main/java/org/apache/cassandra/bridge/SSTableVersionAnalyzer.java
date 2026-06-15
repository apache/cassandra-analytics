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

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes SSTable versions on a cluster to determine the appropriate
 * Cassandra bridge to load for bulk write/read operations.
 *
 * <p>This class provides logic to select Cassandra bridge based on the highest SSTable
 * version detected on the cluster and the user's requested format preference.</p>
 */
public final class SSTableVersionAnalyzer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableVersionAnalyzer.class);
    static final String DISABLE_SSTABLE_VERSION_BASED_BRIDGE =
        "spark.cassandra_analytics.bridge.disable_sstable_version_based";

    private SSTableVersionAnalyzer()
    {
        // Utility class
    }

    /**
     * Determines which CassandraVersion bridge to load based on:
     * - Highest SSTable version detected on cluster
     * - User's format preference
     *
     * @param sstableVersionsOnCluster Set of SSTable versions found on cluster nodes
     * @param requestedFormat User's requested format, example: "big" or "bti"
     * @param cassandraVersion Cassandra version string for fallback
     * @param isSSTableVersionBasedBridgeDisabled flag to disable sstable version based bridge determination
     * @return CassandraVersion enum indicating which bridge to load
     * @throws UnsupportedOperationException if cluster doesn't support requested format
     * @throws IllegalStateException if SSTable versions are empty/unknown
     */
    public static CassandraVersion determineBridgeVersionForWrite(Set<String> sstableVersionsOnCluster,
                                                                  String requestedFormat,
                                                                  String cassandraVersion,
                                                                  boolean isSSTableVersionBasedBridgeDisabled)
    {
        // Check for fallback mode
        Optional<CassandraVersion> fallback = resolveFallbackVersion(cassandraVersion, isSSTableVersionBasedBridgeDisabled);
        if (fallback.isPresent())
        {
            return fallback.get();
        }

        // Validate SSTable versions are present
        ensureSSTableVersionsNotEmpty(sstableVersionsOnCluster);

        // Find highest Cassandra version based on SSTable versions
        CassandraVersion highestCassandraVersion = findHighestCassandraVersion(sstableVersionsOnCluster);

        // Check if highestCassandraVersion supports the requested format
        boolean supportsRequestedFormat = highestCassandraVersion.getNativeSStableVersions()
            .stream()
            .anyMatch(v -> v.startsWith(requestedFormat + "-"));

        if (supportsRequestedFormat)
        {
            return highestCassandraVersion;
        }
        else
        {
            throw new UnsupportedOperationException(String.format(
                          "Cluster does not support requested SSTable format '%s'. " +
                          "Bridge version determined is %s, which only supports formats: %s",
                          requestedFormat, highestCassandraVersion.versionName(),
                          highestCassandraVersion.sstableFormats()));
        }
    }

    /**
     * Determines which CassandraVersion bridge to load for read operations based on:
     * - Highest SSTable version detected on cluster
     *
     * @param sstableVersionsOnCluster Set of SSTable versions found on cluster nodes
     * @param cassandraVersion Cassandra version string for fallback
     * @param isSSTableVersionBasedBridgeDisabled flag to disable sstable version based bridge determination
     * @return CassandraVersion enum indicating which bridge to load
     * @throws IllegalStateException if SSTable versions are empty/unknown
     */
    public static CassandraVersion determineBridgeVersionForRead(Set<String> sstableVersionsOnCluster,
                                                                 String cassandraVersion,
                                                                 boolean isSSTableVersionBasedBridgeDisabled)
    {
        // Check for fallback mode
        Optional<CassandraVersion> fallback = resolveFallbackVersion(cassandraVersion, isSSTableVersionBasedBridgeDisabled);
        if (fallback.isPresent())
        {
            return fallback.get();
        }

        // Validate SSTable versions are present
        ensureSSTableVersionsNotEmpty(sstableVersionsOnCluster);

        // Find highest Cassandra version based on SSTable versions
        CassandraVersion bridgeVersion = findHighestCassandraVersion(sstableVersionsOnCluster);

        LOGGER.debug("Determined bridge version {} for read based on SSTable versions on cluster: {}",
                     bridgeVersion.versionName(), sstableVersionsOnCluster);

        return bridgeVersion;
    }

    private static Optional<CassandraVersion> resolveFallbackVersion(String cassandraVersion,
                                                                      boolean isSSTableVersionBasedBridgeDisabled)
    {
        if (!isSSTableVersionBasedBridgeDisabled)
        {
            return Optional.empty();
        }

        LOGGER.info("SSTable version-based bridge selection is disabled via configuration. " +
                    "Using cassandra.version for bridge selection: {}", cassandraVersion);
        return Optional.of(CassandraVersion.fromVersion(cassandraVersion)
                                           .orElseThrow(() -> new UnsupportedOperationException(
                                           String.format("Unsupported Cassandra version: %s", cassandraVersion))));
    }

    /**
     * Ensures that SSTable versions from cluster are not null or empty.
     *
     * @param sstableVersionsOnCluster Set of SSTable versions to validate
     * @throws IllegalStateException if versions are null or empty
     */
    private static void ensureSSTableVersionsNotEmpty(Set<String> sstableVersionsOnCluster)
    {
        if (sstableVersionsOnCluster == null || sstableVersionsOnCluster.isEmpty())
        {
            throw new IllegalStateException(String.format(
                "Unable to retrieve SSTable versions from cluster. " +
                "This is required for SSTable version-based bridge selection. " +
                "If you want to bypass this check and use cassandra.version for bridge selection, " +
                "set %s=true", DISABLE_SSTABLE_VERSION_BASED_BRIDGE));
        }
    }

    /**
     * Finds the highest Cassandra version based on SSTable versions found on cluster.
     *
     * @param sstableVersionsOnCluster Set of SSTable versions found on cluster
     * @return CassandraVersion corresponding to the highest SSTable version
     * @throws IllegalStateException if highest version is unknown
     */
    private static CassandraVersion findHighestCassandraVersion(Set<String> sstableVersionsOnCluster)
    {
        String highestSSTableVersion = findHighestSSTableVersion(sstableVersionsOnCluster);
        return CassandraVersion.fromSSTableVersion(highestSSTableVersion)
                               .orElseThrow(() -> new IllegalStateException(
                               String.format("Unknown SSTable version: %s. Cannot determine bridge version. " +
                                             "SSTable versions on cluster: %s. " +
                                             "To retry the job using a fallback Cassandra version, " +
                                             "set %s=true",
                                             highestSSTableVersion, sstableVersionsOnCluster,
                                             DISABLE_SSTABLE_VERSION_BASED_BRIDGE)));
    }

    /**
     * Finds the highest SSTable version from the set using CassandraVersion mappings.
     * Ordering is based on CassandraVersion number (e.g., 5.0 > 4.0 > 3.0).
     * Versions within the same CassandraVersion are considered equal.
     *
     * @param versions Set of SSTable version strings
     * @return Highest SSTable version string
     * @throws IllegalStateException if versions is empty, contains null values, or contains unknown versions
     */
    public static String findHighestSSTableVersion(Set<String> versions)
    {
        if (versions == null || versions.isEmpty())
        {
            throw new IllegalStateException("SSTable versions set cannot be empty");
        }

        Comparator<String> sstableVersionComparator = (v1, v2) -> {
            // Find which CassandraVersion each SSTable version belongs to
            Optional<CassandraVersion> v1Opt = CassandraVersion.fromSSTableVersion(v1);
            Optional<CassandraVersion> v2Opt = CassandraVersion.fromSSTableVersion(v2);

            if (!v1Opt.isPresent() || !v2Opt.isPresent())
            {
                String unknownVersion = !v1Opt.isPresent() ? v1 : v2;
                throw new IllegalStateException(
                    String.format("Unknown SSTable version: %s. Cannot determine Cassandra version. " +
                                  "To retry the job using a fallback Cassandra version, " +
                                  "set %s=true", unknownVersion, DISABLE_SSTABLE_VERSION_BASED_BRIDGE));
            }

            CassandraVersion cv1 = v1Opt.get();
            CassandraVersion cv2 = v2Opt.get();

            // First, compare by CassandraVersion number
            // FIVEZERO (50) > FOURONE (41) > FOURZERO (40) > THREEZERO (30)
            int versionComparison = Integer.compare(cv1.versionNumber(), cv2.versionNumber());
            if (versionComparison != 0)
            {
                return versionComparison;
            }

            // Same CassandraVersion - versions are considered equal
            return 0;
        };

        return versions.stream()
            .max(sstableVersionComparator)
            .orElseThrow(() -> new IllegalStateException("Unable to find highest SSTable version"));
    }
}
