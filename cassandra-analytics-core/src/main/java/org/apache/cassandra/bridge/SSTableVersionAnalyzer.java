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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;

/**
 * Determines which Cassandra bridge to load from the SSTable versions present on a cluster.
 */
public final class SSTableVersionAnalyzer
{
    private SSTableVersionAnalyzer()
    {
        // Utility class
    }

    /**
     * Determines the bridge version for bulk write from the SSTable versions present on the cluster.
     * Picks the lowest Cassandra version present so the produced SSTables can be imported by every node.
     *
     * @param sstableVersionsOnCluster set of SSTable versions found on cluster nodes
     * @param requestedFormat          user's requested format, e.g. "big" or "bti"
     * @return the CassandraVersion bridge to load for write
     * @throws IllegalStateException         if the versions are empty/null or contain an unknown version
     * @throws UnsupportedOperationException if the versions are mutually incompatible, or the chosen
     *                                       bridge does not support the requested format
     */
    public static CassandraVersion determineBridgeVersionForWrite(Set<String> sstableVersionsOnCluster, String requestedFormat)
    {
        requireSSTableVersionsPresent(sstableVersionsOnCluster);

        // Every observed version must be mutually compatible (readable by the highest version present).
        ensureMutuallyCompatibleVersions(sstableVersionsOnCluster, highestCompatibleVersion(sstableVersionsOnCluster));

        CassandraVersion bridgeVersion = cassandraVersionsFromSSTableVersions(sstableVersionsOnCluster)
                                         .min(Comparator.comparingInt(CassandraVersion::versionNumber))
                                         .orElseThrow(() -> new IllegalStateException("Unable to determine the lowest SSTable version"));

        if (!bridgeVersion.sstableFormats().contains(requestedFormat))
        {
            throw new UnsupportedOperationException(String.format(
                "Cluster does not support requested SSTable format '%s'. Bridge version determined is %s, "
                + "which only supports formats: %s",
                requestedFormat, bridgeVersion.versionName(), bridgeVersion.sstableFormats()));
        }
        return bridgeVersion;
    }

    /**
     * Determines the bridge version for bulk read from the SSTable versions present on the cluster.
     * Picks the highest Cassandra version present, which can read all (older) versions present.
     *
     * @param sstableVersionsOnCluster set of SSTable versions found on cluster nodes
     * @return the CassandraVersion bridge to load for read
     * @throws IllegalStateException         if the versions are empty/null or contain an unknown version
     * @throws UnsupportedOperationException if the versions are mutually incompatible
     */
    public static CassandraVersion determineBridgeVersionForRead(Set<String> sstableVersionsOnCluster)
    {
        requireSSTableVersionsPresent(sstableVersionsOnCluster);

        CassandraVersion highest = highestCompatibleVersion(sstableVersionsOnCluster);
        ensureMutuallyCompatibleVersions(sstableVersionsOnCluster, highest);
        return highest;
    }

    /**
     * Fails fast with an actionable hint when no SSTable versions could be retrieved while SSTable version-based
     * bridge selection is enabled. Shared by the read and write paths.
     *
     * @param sstableVersionsOnCluster the observed SSTable versions
     * @throws IllegalStateException if {@code sstableVersionsOnCluster} is null or empty
     */
    private static void requireSSTableVersionsPresent(Set<String> sstableVersionsOnCluster)
    {
        if (sstableVersionsOnCluster == null || sstableVersionsOnCluster.isEmpty())
        {
            throw new IllegalStateException(String.format(
                "Unable to retrieve SSTable versions from cluster. This is required for SSTable version-based "
                + "bridge selection. To bypass this check and use cassandra.version for bridge selection, set %s=true",
                BulkSparkConf.DISABLE_SSTABLE_VERSION_BASED_BRIDGE));
        }
    }

    /**
     * Determines the bridge version for a coordinated bulk write across multiple clusters, given the per-cluster
     * bridge versions. Writes at the <em>lowest</em> version present so the produced SSTables are importable by
     * every cluster, but first verifies that lowest version's SSTables can actually be read (imported) by the
     * <em>highest</em> version present; otherwise the clusters span incompatible Cassandra majors and the
     * coordinated write cannot proceed.
     *
     * @param bridgeVersions the per-cluster bridge versions
     * @return the lowest version, which is the version to write at
     * @throws IllegalStateException         if {@code bridgeVersions} is null or empty
     * @throws UnsupportedOperationException if the lowest version's SSTables cannot be imported by the highest
     *                                       version present
     */
    public static CassandraVersion lowestCompatibleWriteVersionForCoordinatedWrites(Collection<CassandraVersion> bridgeVersions)
    {
        if (bridgeVersions == null || bridgeVersions.isEmpty())
        {
            throw new IllegalStateException("No cluster bridge versions available");
        }

        CassandraVersion highest = bridgeVersions.stream()
                                                 .max(Comparator.comparingInt(CassandraVersion::versionNumber))
                                                 .orElseThrow(() -> new IllegalStateException("No cluster bridge versions available"));
        CassandraVersion lowest = bridgeVersions.stream()
                                                .min(Comparator.comparingInt(CassandraVersion::versionNumber))
                                                .orElseThrow(() -> new IllegalStateException("No cluster bridge versions available"));

        // Writing at the lowest version is only safe if the highest version present can import those SSTables.
        if (!highest.canRead(lowest))
        {
            throw new UnsupportedOperationException(String.format(
                "Cluster bridge versions are not mutually compatible for a coordinated write: SSTables written at the "
                + "lowest version present (%s) cannot be imported by the highest version present (%s, which reads %s). "
                + "Per-cluster bridge versions: %s",
                lowest.versionName(), highest.versionName(), highest.getSupportedSStableVersionsForRead(), bridgeVersions));
        }

        return lowest;
    }

    /**
     * Verifies that every observed SSTable version is readable by the given highest version present; otherwise
     * the cluster spans incompatible Cassandra majors and the job cannot proceed.
     *
     * @param sstableVersionsOnCluster the observed SSTable versions
     * @param highest                  the highest Cassandra version present (see {@link #highestCompatibleVersion(Set)})
     * @throws UnsupportedOperationException if any observed version cannot be read by {@code highest}
     */
    private static void ensureMutuallyCompatibleVersions(Set<String> sstableVersionsOnCluster, CassandraVersion highest)
    {
        Set<String> readable = highest.getSupportedSStableVersionsForRead();
        List<String> incompatible = sstableVersionsOnCluster.stream()
                                                            .filter(version -> !readable.contains(version))
                                                            .collect(Collectors.toList());
        if (!incompatible.isEmpty())
        {
            throw new UnsupportedOperationException(String.format(
                "SSTable versions on the cluster are not mutually compatible: %s cannot be read by the highest "
                + "version present (%s, which reads %s). Observed SSTable versions: %s",
                incompatible, highest.versionName(), readable, sstableVersionsOnCluster));
        }
    }

    /**
     * Returns the highest Cassandra version among the observed SSTable versions.
     *
     * @throws IllegalStateException if the versions are empty/null or contain an unknown version
     */
    private static CassandraVersion highestCompatibleVersion(Set<String> sstableVersionsOnCluster)
    {
        return cassandraVersionsFromSSTableVersions(sstableVersionsOnCluster)
               .max(Comparator.comparingInt(CassandraVersion::versionNumber))
               .orElseThrow(() -> new IllegalStateException("Unable to determine the highest SSTable version"));
    }

    private static Stream<CassandraVersion> cassandraVersionsFromSSTableVersions(Set<String> sstableVersionsOnCluster)
    {
        if (sstableVersionsOnCluster == null || sstableVersionsOnCluster.isEmpty())
        {
            throw new IllegalStateException("Unable to determine Cassandra versions: no SSTable versions found on the cluster");
        }

        return sstableVersionsOnCluster.stream().map(SSTableVersionAnalyzer::toCassandraVersion);
    }

    private static CassandraVersion toCassandraVersion(String sstableVersion)
    {
        return CassandraVersion.fromSSTableVersion(sstableVersion)
                               .orElseThrow(() -> new IllegalStateException("Unknown SSTable version: " + sstableVersion));
    }
}
