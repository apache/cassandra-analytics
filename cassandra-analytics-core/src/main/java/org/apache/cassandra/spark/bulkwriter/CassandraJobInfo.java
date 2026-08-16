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

import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.utils.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CassandraJobInfo implements JobInfo
{
    protected final BulkSparkConf conf;
    // restoreJobId per cluster; it is guaranteed to be non-empty
    protected final MultiClusterContainer<UUID> restoreJobIds;
    protected final TokenPartitioner tokenPartitioner;

    public CassandraJobInfo(BulkSparkConf conf, MultiClusterContainer<UUID> restoreJobIds, TokenPartitioner tokenPartitioner)
    {
        Preconditions.checkArgument(restoreJobIds.size() > 0, "restoreJobIds cannot be empty");
        this.restoreJobIds = restoreJobIds;
        this.conf = conf;
        this.tokenPartitioner = tokenPartitioner;
    }

    /**
     * Reconstruct from BroadcastableJobInfo on executor.
     * Reuses conf and restoreJobIds from broadcast,
     * and reconstructs TokenPartitioner from broadcastable partition mappings.
     *
     * @param broadcastable the broadcastable job info from broadcast
     */
    public CassandraJobInfo(BroadcastableJobInfo broadcastable)
    {
        this.conf = broadcastable.getConf();
        this.restoreJobIds = broadcastable.getRestoreJobIds();
        // Reconstruct TokenPartitioner from broadcastable partition mappings
        this.tokenPartitioner = new TokenPartitioner(broadcastable.getBroadcastableTokenPartitioner());
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return conf.consistencyLevel;
    }

    @Override
    public String getLocalDC()
    {
        if (isCoordinatedWriteEnabled())
        {
            throw new UnsupportedOperationException("Method not supported, when coordinated write is enabled. " +
                                                    "Call-sites should get localDc from #coordinatedWriteConf() method");
        }
        return conf.localDC;
    }

    @Override
    public int sstableDataSizeInMiB()
    {
        return conf.sstableDataSizeInMiB;
    }

    @Override
    public int getCommitBatchSize()
    {
        return conf.commitBatchSize;
    }

    @Override
    public boolean skipExtendedVerify()
    {
        return conf.skipExtendedVerify;
    }

    @Override
    public boolean skipRowsViolatingConstraints()
    {
        return conf.skipRowsViolatingConstraints;
    }

    @Override
    public boolean getSkipClean()
    {
        return conf.getSkipClean();
    }

    @Override
    public DataTransportInfo transportInfo()
    {
        return conf.getTransportInfo();
    }

    @Override
    public int jobKeepAliveMinutes()
    {
        return conf.getJobKeepAliveMinutes();
    }

    @Override
    public long jobTimeoutSeconds()
    {
        return conf.getJobTimeoutSeconds();
    }

    @Override
    public int effectiveSidecarPort()
    {
        return conf.getEffectiveSidecarPort();
    }

    @Override
    public double importCoordinatorTimeoutMultiplier()
    {
        return conf.importCoordinatorTimeoutMultiplier;
    }

    @Nullable
    @Override
    public CoordinatedWriteConf coordinatedWriteConf()
    {
        return conf.coordinatedWriteConf();
    }

    @Override
    public int getCommitThreadsPerInstance()
    {
        return conf.commitThreadsPerInstance;
    }

    @Override
    public UUID getRestoreJobId()
    {
        // get the id assume it is single cluster scenario; using `null` to retrieve.
        try
        {
            return getRestoreJobId(null);
        }
        catch (NoSuchElementException nsee)
        {
            // if it is multi-cluster scenario, id would be null; in this case, just retrieve a value
            return restoreJobIds.getAnyValue();
        }
    }

    @Override
    public UUID getRestoreJobId(@Nullable String clusterId)
    {
        return restoreJobIds.getValueOrThrow(clusterId);
    }

    @Override
    public String getConfiguredJobId()
    {
        return conf.configuredJobId;
    }

    @Override
    public TokenPartitioner getTokenPartitioner()
    {
        return tokenPartitioner;
    }

    @NotNull
    @Override
    public DigestAlgorithmSupplier digestAlgorithmSupplier()
    {
        return conf.digestAlgorithmSupplier;
    }

    @NotNull
    public QualifiedTableName qualifiedTableName()
    {
        return new QualifiedTableName(conf.keyspace, conf.table, conf.quoteIdentifiers);
    }
}
