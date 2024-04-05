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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.utils.RangeUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.MultipleReplicas;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.SingleReplica;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DataLayer that partitions token range by the number of Spark partitions
 * and only lists SSTables overlapping with range
 */
@SuppressWarnings("WeakerAccess")
public abstract class PartitionedDataLayer extends DataLayer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedDataLayer.class);
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;

    @NotNull
    protected ConsistencyLevel consistencyLevel;
    protected String datacenter;

    public enum AvailabilityHint
    {
        // 0 means high priority
        UP(0), MOVING(1), LEAVING(1), UNKNOWN(2), JOINING(2), DOWN(2);

        private final int priority;

        AvailabilityHint(int priority)
        {
            this.priority = priority;
        }

        public static final Comparator<AvailabilityHint> AVAILABILITY_HINT_COMPARATOR =
                Comparator.comparingInt((AvailabilityHint other) -> other.priority).reversed();

        public static AvailabilityHint fromState(String status, String state)
        {
            if (status.equalsIgnoreCase(AvailabilityHint.DOWN.name()))
            {
                return AvailabilityHint.DOWN;
            }

            if (status.equalsIgnoreCase(AvailabilityHint.UNKNOWN.name()))
            {
                return AvailabilityHint.UNKNOWN;
            }

            if (state.equalsIgnoreCase("NORMAL"))
            {
                return AvailabilityHint.valueOf(status.toUpperCase());
            }
            if (state.equalsIgnoreCase(AvailabilityHint.MOVING.name()))
            {
                return AvailabilityHint.MOVING;
            }
            if (state.equalsIgnoreCase(AvailabilityHint.LEAVING.name()))
            {
                return AvailabilityHint.LEAVING;
            }
            if (state.equalsIgnoreCase("STARTING"))
            {
                return AvailabilityHint.valueOf(status.toUpperCase());
            }
            if (state.equalsIgnoreCase(AvailabilityHint.JOINING.name()))
            {
                return AvailabilityHint.JOINING;
            }

            return AvailabilityHint.UNKNOWN;
        }
    }

    public PartitionedDataLayer(@Nullable ConsistencyLevel consistencyLevel, @Nullable String datacenter)
    {
        this.consistencyLevel = consistencyLevel != null ? consistencyLevel : DEFAULT_CONSISTENCY_LEVEL;
        this.datacenter = datacenter;

        if (consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL)
        {
            throw new IllegalArgumentException("SERIAL or LOCAL_SERIAL are invalid consistency levels for the Bulk Reader");
        }
        if (consistencyLevel == ConsistencyLevel.EACH_QUORUM)
        {
            throw new UnsupportedOperationException("EACH_QUORUM has not been implemented yet");
        }
    }

    protected void validateReplicationFactor(@NotNull ReplicationFactor replicationFactor)
    {
        validateReplicationFactor(consistencyLevel, replicationFactor, datacenter);
    }

    @VisibleForTesting
    public static void validateReplicationFactor(@NotNull ConsistencyLevel consistencyLevel,
                                                 @NotNull ReplicationFactor replicationFactor,
                                                 @Nullable String dc)
    {
        if (replicationFactor.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            return;
        }
        // Single DC and no DC specified so use only DC in replication factor
        if (dc == null && replicationFactor.getOptions().size() == 1)
        {
            return;
        }
        Preconditions.checkArgument(dc != null || !consistencyLevel.isDCLocal,
                                    "A DC must be specified for DC local consistency level " + consistencyLevel.name());
        if (dc == null)
        {
            return;
        }
        Preconditions.checkArgument(replicationFactor.getOptions().containsKey(dc),
                                    "DC %s not found in replication factor %s",
                                    dc, replicationFactor.getOptions().keySet());
        Preconditions.checkArgument(replicationFactor.getOptions().get(dc) > 0,
                                    "Cannot read from DC %s with non-positive replication factor %d",
                                    dc, replicationFactor.getOptions().get(dc));
    }

    public abstract CompletableFuture<Stream<SSTable>> listInstance(int partitionId, @NotNull Range<BigInteger> range, @NotNull CassandraInstance instance);

    public abstract CassandraRing ring();

    public abstract TokenPartitioner tokenPartitioner();

    @Override
    public int partitionCount()
    {
        return tokenPartitioner().numPartitions();
    }

    @Override
    public Partitioner partitioner()
    {
        return ring().partitioner();
    }

    @Override
    public boolean isInPartition(int partitionId, BigInteger token, ByteBuffer key)
    {
        return tokenPartitioner().isInPartition(token, key, partitionId);
    }

    @Override
    public SparkRangeFilter sparkRangeFilter(int partitionId)
    {
        Map<Integer, Range<BigInteger>> reversePartitionMap = tokenPartitioner().reversePartitionMap();
        Range<BigInteger> sparkTokenRange = reversePartitionMap.get(partitionId);
        if (sparkTokenRange == null)
        {
            LOGGER.error("Unable to find the sparkTokenRange for partitionId={} in reversePartitionMap={}",
                         partitionId, reversePartitionMap);
            throw new IllegalStateException(
                    String.format("Unable to find sparkTokenRange for partitionId=%d in the reverse partition map",
                                  partitionId));
        }
        return SparkRangeFilter.create(RangeUtils.toTokenRange(sparkTokenRange));
    }

    @Override
    public List<PartitionKeyFilter> partitionKeyFiltersInRange(
            int partitionId,
            List<PartitionKeyFilter> filters) throws NoMatchFoundException
    {
        // We only need to worry about Partition key filters that overlap with this Spark workers token range
        SparkRangeFilter rangeFilter = sparkRangeFilter(partitionId);
        TokenRange sparkTokenRange = rangeFilter.tokenRange();

        List<PartitionKeyFilter> filtersInRange = filters.stream()
                                                         .filter(filter -> filter.overlaps(sparkTokenRange))
                                                         .collect(Collectors.toList());

        if (!filters.isEmpty() && filtersInRange.isEmpty())
        {
            LOGGER.info("None of the partition key filters overlap with Spark partition token range firstToken={} lastToken={}",
                        sparkTokenRange.firstEnclosedValue(), sparkTokenRange.upperEndpoint());
            throw new NoMatchFoundException();
        }
        return filterNonIntersectingSSTables() ? filtersInRange : filters;
    }

    public ConsistencyLevel consistencylevel()
    {
        return consistencyLevel;
    }

    @Override
    public SSTablesSupplier sstables(int partitionId,
                                     @Nullable SparkRangeFilter sparkRangeFilter,
                                     @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        // Get token range for Spark partition
        TokenPartitioner tokenPartitioner = tokenPartitioner();
        if (partitionId < 0 || partitionId >= tokenPartitioner.numPartitions())
        {
            throw new IllegalStateException("PartitionId outside expected range: " + partitionId);
        }

        // Get all replicas overlapping partition token range
        Range<BigInteger> range = tokenPartitioner.getTokenRange(partitionId);
        CassandraRing ring = ring();
        ReplicationFactor replicationFactor = ring.replicationFactor();
        validateReplicationFactor(replicationFactor);
        Map<Range<BigInteger>, List<CassandraInstance>> instRanges;
        Map<Range<BigInteger>, List<CassandraInstance>> subRanges = ring().getSubRanges(range).asMapOfRanges();
        if (partitionKeyFilters.isEmpty())
        {
            instRanges = subRanges;
        }
        else
        {
            instRanges = new HashMap<>();
            subRanges.keySet().forEach(instRange -> {
                TokenRange tokenRange = RangeUtils.toTokenRange(instRange);
                if (partitionKeyFilters.stream().anyMatch(filter -> filter.overlaps(tokenRange)))
                {
                    instRanges.putIfAbsent(instRange, subRanges.get(instRange));
                }
            });
        }

        Set<CassandraInstance> replicas = PartitionedDataLayer.rangesToReplicas(consistencyLevel, datacenter, instRanges);
        LOGGER.info("Creating partitioned SSTablesSupplier for Spark partition partitionId={} rangeLower={} rangeUpper={} numReplicas={}",
                    partitionId, range.lowerEndpoint(), range.upperEndpoint(), replicas.size());

        // Use consistency level and replication factor to calculate min number of replicas required
        // to satisfy consistency level; split replicas into 'primary' and 'backup' replicas,
        // attempt on primary replicas and use backups to retry in the event of a failure
        int minReplicas = consistencyLevel.blockFor(replicationFactor, datacenter);
        ReplicaSet replicaSet = PartitionedDataLayer.splitReplicas(
                consistencyLevel, datacenter, instRanges, replicas, this::getAvailability, minReplicas, partitionId);

        if (replicaSet.primary().size() < minReplicas)
        {
            // Could not find enough primary replicas to meet consistency level
            assert replicaSet.backup().isEmpty();
            throw new NotEnoughReplicasException(consistencyLevel, range, minReplicas, replicas.size(), datacenter);
        }

        ExecutorService executor = executorService();
        Stats stats = stats();
        Set<SingleReplica> primaryReplicas = replicaSet.primary().stream()
                .map(instance -> new SingleReplica(instance, this, range, partitionId, executor, stats, replicaSet.isRepairPrimary(instance)))
                .collect(Collectors.toSet());
        Set<SingleReplica> backupReplicas = replicaSet.backup().stream()
                .map(instance -> new SingleReplica(instance, this, range, partitionId, executor, stats, true))
                .collect(Collectors.toSet());

        return new MultipleReplicas(primaryReplicas, backupReplicas, stats);
    }

    /**
     * Overridable method setting whether the PartitionedDataLayer should filter out SSTables
     * that do not intersect with the Spark partition token range
     *
     * @return true if we should filter
     */
    public boolean filterNonIntersectingSSTables()
    {
        return true;
    }

    /**
     * Data Layer can override this method to hint availability of a Cassandra instance so Bulk Reader attempts
     * UP instances first, and avoids instances known to be down e.g. if create snapshot request already failed
     *
     * @param instance a cassandra instance
     * @return availability hint
     */
    protected AvailabilityHint getAvailability(CassandraInstance instance)
    {
        return AvailabilityHint.UNKNOWN;
    }

    static Set<CassandraInstance> rangesToReplicas(@NotNull ConsistencyLevel consistencyLevel,
                                                   @Nullable String dataCenter,
                                                   @NotNull Map<Range<BigInteger>, List<CassandraInstance>> ranges)
    {
        return ranges.values().stream()
                .flatMap(Collection::stream)
                .filter(instance -> !consistencyLevel.isDCLocal || dataCenter == null || instance.dataCenter().equals(dataCenter))
                .collect(Collectors.toSet());
    }

    /**
     * Split the replicas overlapping with the Spark worker's token range based on availability hint so that we
     * achieve consistency
     *
     * @param consistencyLevel user set consistency level
     * @param dataCenter       data center to read from
     * @param ranges           all the token ranges owned by this Spark worker, and associated replicas
     * @param replicas         all the replicas we can read from
     * @param availability     availability hint provider for each CassandraInstance
     * @param minReplicas      minimum number of replicas to achieve consistency
     * @param partitionId      Spark worker partitionId
     * @return a set of primary and backup replicas to read from
     * @throws NotEnoughReplicasException thrown when insufficient primary replicas selected to achieve
     *                                    consistency level for any sub-range of the Spark worker's token range
     */
    static ReplicaSet splitReplicas(@NotNull ConsistencyLevel consistencyLevel,
                                    @Nullable String dataCenter,
                                    @NotNull Map<Range<BigInteger>, List<CassandraInstance>> ranges,
                                    @NotNull Set<CassandraInstance> replicas,
                                    @NotNull Function<CassandraInstance, AvailabilityHint> availability,
                                    int minReplicas,
                                    int partitionId) throws NotEnoughReplicasException
    {
        ReplicaSet split = splitReplicas(replicas, ranges, availability, minReplicas, partitionId);
        validateConsistency(consistencyLevel, dataCenter, ranges, split.primary(), minReplicas);
        return split;
    }

    /**
     * Validate we have achieved consistency for all sub-ranges owned by the Spark worker
     *
     * @param consistencyLevel consistency level
     * @param dc               data center
     * @param workerRanges     token sub-ranges owned by this Spark worker
     * @param primaryReplicas  set of primary replicas selected
     * @param minReplicas      minimum number of replicas required to meet consistency level
     * @throws NotEnoughReplicasException thrown when insufficient primary replicas selected to achieve
     *                                    consistency level for any sub-range of the Spark worker's token range
     */
    private static void validateConsistency(@NotNull ConsistencyLevel consistencyLevel,
                                            @Nullable String dc,
                                            @NotNull Map<Range<BigInteger>, List<CassandraInstance>> workerRanges,
                                            @NotNull Set<CassandraInstance> primaryReplicas,
                                            int minReplicas) throws NotEnoughReplicasException
    {
        for (Map.Entry<Range<BigInteger>, List<CassandraInstance>> range : workerRanges.entrySet())
        {
            int count = (int) range.getValue().stream().filter(primaryReplicas::contains).count();
            if (count < minReplicas)
            {
                throw new NotEnoughReplicasException(consistencyLevel, range.getKey(), minReplicas, count, dc);
            }
        }
    }

    /**
     * Return a set of primary and backup CassandraInstances to satisfy the consistency level.
     *
     * NOTE: This method current assumes that each Spark token worker owns a single replica set.
     *
     * @param instances    replicas that overlap with the Spark worker's token range
     * @param ranges       all the token ranges owned by this Spark worker, and associated replicas
     * @param availability availability hint provider for each CassandraInstance
     * @param minReplicas  minimum number of replicas to achieve consistency
     * @param partitionId  Spark worker partitionId
     * @return a set of primary and backup replicas to read from
     */
    static ReplicaSet splitReplicas(Collection<CassandraInstance> instances,
                                    @NotNull Map<Range<BigInteger>, List<CassandraInstance>> ranges,
                                    Function<CassandraInstance, AvailabilityHint> availability,
                                    int minReplicas,
                                    int partitionId)
    {
        ReplicaSet replicaSet = new ReplicaSet(minReplicas, partitionId);

        // Sort instances by status hint, so we attempt available instances first
        // (e.g. we already know which instances are probably up from create snapshot request)
        instances.stream()
                 .sorted(Comparator.comparing(availability, AvailabilityHint.AVAILABILITY_HINT_COMPARATOR))
                 .forEach(replicaSet::add);

        if (ranges.size() != 1)
        {
            // Currently we don't support using incremental repair when Spark worker owns
            // multiple replica sets but for current implementation of the TokenPartitioner
            // it returns a single replica set per Spark worker/partition
            LOGGER.warn("Cannot use incremental repair awareness when Spark partition owns more than one replica set, "
                      + "performance will be degraded numRanges={}", ranges.size());
            replicaSet.incrementalRepairPrimary = null;
        }

        return replicaSet;
    }

    public static class ReplicaSet
    {
        private final Set<CassandraInstance> primary;
        private final Set<CassandraInstance> backup;
        private final int minReplicas;
        private final int partitionId;
        private CassandraInstance incrementalRepairPrimary;

        ReplicaSet(int minReplicas,
                   int partitionId)
        {
            this.minReplicas = minReplicas;
            this.partitionId = partitionId;
            this.primary = new HashSet<>();
            this.backup = new HashSet<>();
        }

        public ReplicaSet add(CassandraInstance instance)
        {
            if (primary.size() < minReplicas)
            {
                LOGGER.info("Selecting instance as primary replica nodeName={} token={} dc={} partitionId={}",
                            instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
                return addPrimary(instance);
            }
            return addBackup(instance);
        }

        public boolean isRepairPrimary(CassandraInstance instance)
        {
            return incrementalRepairPrimary == null || incrementalRepairPrimary.equals(instance);
        }

        public Set<CassandraInstance> primary()
        {
            return primary;
        }

        public ReplicaSet addPrimary(CassandraInstance instance)
        {
            if (incrementalRepairPrimary == null)
            {
                // Pick the first primary replica as a 'repair primary' to read repaired SSTables at CL ONE
                incrementalRepairPrimary = instance;
            }
            primary.add(instance);
            return this;
        }

        public Set<CassandraInstance> backup()
        {
            return backup;
        }

        public ReplicaSet addBackup(CassandraInstance instance)
        {
            LOGGER.info("Selecting instance as backup replica nodeName={} token={} dc={} partitionId={}",
                        instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
            backup.add(instance);
            return this;
        }
    }

    public abstract ReplicationFactor replicationFactor(String keyspace);

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(datacenter)
               .toHashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        PartitionedDataLayer that = (PartitionedDataLayer) other;
        return new EqualsBuilder()
               .append(this.datacenter, that.datacenter)
               .isEquals();
    }
}
