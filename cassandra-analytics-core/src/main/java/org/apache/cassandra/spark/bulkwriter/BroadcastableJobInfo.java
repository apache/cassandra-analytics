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

import java.util.UUID;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Broadcastable data wrapper for broadcasting with ZERO transient fields.
 * Only essential fields are broadcast; executors reconstruct CassandraJobInfo to rebuild TokenPartitioner.
 * NO LOGGER - to avoid logger references in broadcast variable.
 */
public final class BroadcastableJobInfo implements Serializable
{
    // Essential fields broadcast to executors
    private final BulkSparkConf conf;
    private final MultiClusterContainer<UUID> restoreJobIds;
    private final BroadcastableTokenPartitioner tokenPartitioner;  // Broadcastable version without Logger

    /**
     * Creates a BroadcastableJobInfo from a source JobInfo.
     * Extracts partition mappings from TokenPartitioner to avoid broadcasting Logger.
     *
     * @param source the source JobInfo (typically CassandraJobInfo)
     * @param conf   the BulkSparkConf needed for executors
     */
    public static BroadcastableJobInfo from(@NotNull JobInfo source, @NotNull BulkSparkConf conf)
    {
        // Extract restoreJobIds from source
        MultiClusterContainer<UUID> restoreJobIds;
        if (source.isCoordinatedWriteEnabled())
        {
            // For coordinated write, need to extract all restoreJobIds
            CoordinatedWriteConf coordinatedConf = source.coordinatedWriteConf();
            restoreJobIds = new MultiClusterContainer<>();
            coordinatedConf.clusters().keySet().forEach(clusterId -> {
                restoreJobIds.setValue(clusterId, source.getRestoreJobId(clusterId));
            });
        }
        else
        {
            // Single cluster - use null key
            restoreJobIds = MultiClusterContainer.ofSingle(source.getRestoreJobId());
        }

        // Extract partition mappings from TokenPartitioner
        BroadcastableTokenPartitioner broadcastableTokenPartitioner = BroadcastableTokenPartitioner.from(source.getTokenPartitioner());

        return new BroadcastableJobInfo(conf, restoreJobIds, broadcastableTokenPartitioner);
    }

    private BroadcastableJobInfo(BulkSparkConf conf,
                                MultiClusterContainer<UUID> restoreJobIds,
                                BroadcastableTokenPartitioner tokenPartitioner)
    {
        this.conf = conf;
        this.restoreJobIds = restoreJobIds;
        this.tokenPartitioner = tokenPartitioner;
    }

    public BulkSparkConf getConf()
    {
        return conf;
    }

    public MultiClusterContainer<UUID> getRestoreJobIds()
    {
        return restoreJobIds;
    }

    public BroadcastableTokenPartitioner getBroadcastableTokenPartitioner()
    {
        return tokenPartitioner;
    }
}
