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

package org.apache.cassandra.spark.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;

/**
 * Dynamic {@link Sizing} implementation that uses table size, minimum number of replicas, maximum partition size,
 * and available Spark cores to determine the effective number of executor cores to use during the spark job execution.
 *
 * <p>This class is typically used when the table size is relatively small (few GBs). When reading small datasets,
 * this class will allocate a limited number of resources to read the table. This in turn helps reduce the cost of
 * coordinating a large number of executor cores when the dataset does not justify using the entire spark cluster
 * for reading.
 */
public class DynamicSizing implements Sizing
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSizing.class);

    private final ReplicationFactor replicationFactor;
    private final int maxPartitionSize;
    private final int availableCores;
    private final String keyspace;
    private final String table;
    private final String dc;
    private final TableSizeProvider tableSizeProvider;
    private final ConsistencyLevel consistencyLevel;

    /**
     * Constructs a new Sizing object.
     *
     * @param tableSizeProvider the table size provider
     * @param consistencyLevel  the consistency level for the read operation
     * @param replicationFactor the replication factor for the keyspace
     * @param keyspace          the Cassandra keyspace
     * @param table             the Cassandra table
     * @param datacenter        the Cassandra datacenter
     * @param maxPartitionSize  the maximum partition size desired
     * @param availableCores    the maximum number of cores available
     */
    public DynamicSizing(TableSizeProvider tableSizeProvider,
                         ConsistencyLevel consistencyLevel,
                         ReplicationFactor replicationFactor,
                         String keyspace,
                         String table,
                         String datacenter,
                         int maxPartitionSize,
                         int availableCores)
    {
        this.tableSizeProvider = tableSizeProvider;
        this.consistencyLevel = consistencyLevel;
        this.replicationFactor = replicationFactor;
        this.keyspace = keyspace;
        this.table = table;
        this.dc = datacenter;
        this.maxPartitionSize = maxPartitionSize;
        this.availableCores = availableCores;
    }

    /**
     * Returns the effective number of cores to be used during the spark execution.
     * The value is calculated by getting the table size * the number of replicas
     * we will use to read the data and then dividing it by the maximum partition
     * size in GB. For example, assume we have a table with 7.25 GB of data, and
     * assume a maximum partition size of 2.5 GB. Also, assume that a consistency
     * level of 2. The number of cores is calculated by the following formula:
     *
     * <pre>
     *                                             totalTableSize * minReplicas
     *     effectiveNumberOfCores = Math.ceil( ------------------------------------- )
     *                                                 maxPartitionSize
     * </pre>
     *
     * <p>In the example above, we have:
     *
     * <pre>
     *                                7.25 GB * 2
     *     effectiveNumberOfCores = --------------- = 5.8 ~&gt; 6 cores
     *                                  2.5 GB
     * </pre>
     *
     * <p>This method is guaranteed to return at least 1 core and at most {@code availableCores}
     *
     * @return the effective number of cores to be used during the spark execution
     */
    @Override
    public int getEffectiveNumberOfCores()
    {
        double tableSizeInGiB = ((double) tableSizeProvider.tableSizeInBytes(keyspace, table, dc)
                                 / (double) (1024 /* KiB */ * 1024 /* MiB */ * 1024 /* GiB */));
        double minReplicas = consistencyLevel.blockFor(replicationFactor, dc);

        // Guarantee at least one core and at most availableCores
        int effectiveNumberOfCores = Math.min(Math.max(1, (int) Math.ceil(tableSizeInGiB * minReplicas / maxPartitionSize)), availableCores);

        LOGGER.info("Using Dynamic Sizing. tableSize {}GiB, minReplicas {}, maxPartitionSize {}GiB, availableCores {}, effectiveNumberOfCores {}",
                    tableSizeInGiB, minReplicas, maxPartitionSize, availableCores, effectiveNumberOfCores);

        return effectiveNumberOfCores;
    }
}
