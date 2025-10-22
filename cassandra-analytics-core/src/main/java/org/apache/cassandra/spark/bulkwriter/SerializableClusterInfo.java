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

package org.apache.cassandra.spark.bulkwriter;

import java.math.BigInteger;
import java.util.Map;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Serializable implementation of ClusterInfo with ZERO transient fields.
 * This class is designed for Spark broadcasting to avoid SizeEstimator overhead from inspecting transient fields.
 * All data is pre-computed on the driver and stored in final fields.
 */
public final class SerializableClusterInfo implements ClusterInfo
{
    private static final long serialVersionUID = 1L;

    // All fields are final and non-transient - no lazy loading
    private final Partitioner partitioner;
    private final String cassandraVersion;
    private final String clusterId;
    private final ReplicationFactor replicationFactor;
    private final TokenRangeMapping<RingInstance> tokenRangeMapping;
    private final Map<RingInstance, WriteAvailability> writeAvailability;
    private final String keyspaceSchema;

    // Configuration to rebuild CassandraContext on executors
    // CassandraContext itself has transient fields, so we store config and rebuild it lazily
    private final BulkSparkConf conf;
    // Volatile for lazy initialization with double-checked locking
    private volatile CassandraContext cassandraContext;

    /**
     * Creates a SerializableClusterInfo from a CassandraClusterInfo by extracting all necessary pre-computed data.
     *
     * @param source the source ClusterInfo (typically CassandraClusterInfo)
     * @param conf   the BulkSparkConf needed to rebuild CassandraContext on executors
     */
    public static SerializableClusterInfo from(@NotNull ClusterInfo source, @NotNull BulkSparkConf conf)
    {
        return new SerializableClusterInfo(
            source.getPartitioner(),
            source.getLowestCassandraVersion(),
            source.clusterId(),
            source.replicationFactor(),
            source.getTokenRangeMapping(true),
            source.clusterWriteAvailability(),
            source.getKeyspaceSchema(true),
            conf,
            source.getCassandraContext()
        );
    }

    private SerializableClusterInfo(@NotNull Partitioner partitioner,
                                    @NotNull String cassandraVersion,
                                    @Nullable String clusterId,
                                    @NotNull ReplicationFactor replicationFactor,
                                    @NotNull TokenRangeMapping<RingInstance> tokenRangeMapping,
                                    @NotNull Map<RingInstance, WriteAvailability> writeAvailability,
                                    @NotNull String keyspaceSchema,
                                    @NotNull BulkSparkConf conf,
                                    @NotNull CassandraContext ignoredContext)
    {
        this.partitioner = partitioner;
        this.cassandraVersion = cassandraVersion;
        this.clusterId = clusterId;
        this.replicationFactor = replicationFactor;
        this.tokenRangeMapping = tokenRangeMapping;
        this.writeAvailability = writeAvailability;
        this.keyspaceSchema = keyspaceSchema;
        this.conf = conf;
        // Don't store cassandraContext - it will be rebuilt lazily on executors
        // This avoids serializing CassandraContext which has a transient sidecarClient field
        this.cassandraContext = null;
    }

    @Override
    public void refreshClusterInfo()
    {
        // No-op - this is an immutable snapshot
    }

    @Override
    public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
    {
        return tokenRangeMapping;
    }

    @Override
    public String getLowestCassandraVersion()
    {
        return cassandraVersion;
    }

    @Override
    public Map<RingInstance, WriteAvailability> clusterWriteAvailability()
    {
        return writeAvailability;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public void checkBulkWriterIsEnabledOrThrow()
    {
        // No-op - validation already done on driver
    }

    @Override
    public void validateTimeSkew(Range<BigInteger> range) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        // No-op - validation already done on driver or will be done via CassandraContext
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        return keyspaceSchema;
    }

    @Override
    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
    }

    @Override
    public CassandraContext getCassandraContext()
    {
        // Lazy initialization with double-checked locking
        if (cassandraContext == null)
        {
            synchronized (this)
            {
                if (cassandraContext == null)
                {
                    cassandraContext = CassandraContext.create(conf, clusterId);
                }
            }
        }
        return cassandraContext;
    }

    @Override
    @Nullable
    public String clusterId()
    {
        return clusterId;
    }

    @Override
    public void startupValidate()
    {
        // No-op - validation already done on driver
    }

    @Override
    public void close()
    {
        // Delegate to CassandraContext
        if (cassandraContext != null)
        {
            cassandraContext.close();
        }
    }
}
