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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.utils.RangeUtils;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.spark.Partitioner;

/**
 * Spark Partitioner for distributing data across Cassandra token ranges.
 * <p>
 * Serialization Architecture:
 * This class supports TWO distinct serialization mechanisms, each serving a different purpose:
 * <p>
 * 1. <b>Direct Java Serialization (via writeObject/readObject)</b>:
 *    Used when Spark serializes this Partitioner for shuffle operations like
 *    {@code repartitionAndSortWithinPartitions()}. During shuffle, Spark sends the Partitioner
 *    to executors to determine which partition each record belongs to. The custom serialization
 *    methods at the end of this class handle saving/restoring the partition mappings.
 * <p>
 * 2. <b>Broadcast Variable Pattern (via BroadcastableTokenPartitioner)</b>:
 *    Used when broadcasting job configuration to executors. The driver extracts partition mappings
 *    into {@link BroadcastableTokenPartitioner} (a pure data wrapper with no transient fields),
 *    which is broadcast via {@link BulkWriterConfig}. Executors reconstruct TokenPartitioner from
 *    the broadcast data using the constructor {@link #TokenPartitioner(BroadcastableTokenPartitioner)}.
 * <p>
 * Both mechanisms are necessary because:
 * - Shuffle operations (repartitionAndSortWithinPartitions) serialize the Partitioner directly
 * - Broadcast variables use the broadcastable wrapper pattern to avoid Logger serialization issues
 * <p>
 * The transient fields (partitionMap, reversePartitionMap, nrPartitions) are marked transient to
 * avoid serializing large/complex objects when not needed, but are properly handled by custom
 * serialization when direct serialization is required.
 */
public class TokenPartitioner extends Partitioner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenPartitioner.class);
    private static final long serialVersionUID = -8787074052066841747L;

    private transient int nrPartitions;
    private transient RangeMap<BigInteger, Integer> partitionMap;
    private transient Map<Integer, Range<BigInteger>> reversePartitionMap;

    private final transient TokenRangeMapping<RingInstance> tokenRangeMapping;
    private final Integer numberSplits;

    public TokenPartitioner(TokenRangeMapping<RingInstance> tokenRangeMapping,
                            Integer userSpecifiedNumberSplits,
                            int defaultParallelism,
                            Integer cores)
    {
        this(tokenRangeMapping, userSpecifiedNumberSplits, defaultParallelism, cores, true);
    }

    @VisibleForTesting
    public TokenPartitioner(TokenRangeMapping<RingInstance> tokenRangeMapping,
                            Integer userSpecifiedNumberSplits,
                            int defaultParallelism,
                            Integer cores,
                            boolean randomize)
    {
        this.tokenRangeMapping = tokenRangeMapping;
        this.numberSplits = calculateSplits(tokenRangeMapping, userSpecifiedNumberSplits, defaultParallelism, cores);
        setupTokenRangeMap(randomize);
        validate(); // Intentionally keeping validation in the driver alone; there is no need to re-validate when constructing in executors
        logPartitionInfo();
    }

    private void logPartitionInfo()
    {
        LOGGER.debug("Number of partitions: {}", nrPartitions);
        LOGGER.debug("Partition map: {}", partitionMap);
        LOGGER.debug("Reverse partition map: {}", reversePartitionMap);
    }

    /**
     * Reconstruct TokenPartitioner from BroadcastableTokenPartitioner on executor.
     * <p>
     * This constructor is part of the <b>broadcast variable</b> serialization mechanism.
     * When BulkWriterConfig is broadcast to executors, it contains BroadcastableTokenPartitioner
     * (a pure data wrapper). Executors use this constructor to rebuild the TokenPartitioner
     * with all necessary partition mappings.
     * <p>
     * This reconstruction path is separate from the direct Java serialization (writeObject/readObject)
     * used for Spark shuffle operations. The broadcast pattern is preferred for configuration data
     * because it avoids Logger serialization issues and minimizes broadcast size.
     *
     * @param broadcastable the broadcastable token partitioner from broadcast variable
     * @see BroadcastableTokenPartitioner
     * @see BulkWriterConfig
     */
    public TokenPartitioner(BroadcastableTokenPartitioner broadcastable)
    {
        this.tokenRangeMapping = null;  // Not needed on executors
        this.numberSplits = broadcastable.numSplits();
        this.partitionMap = com.google.common.collect.TreeRangeMap.create();
        this.reversePartitionMap = new HashMap<>();
        this.nrPartitions = 0;

        // Restore partition mappings from serialized form
        broadcastable.getPartitionEntries().forEach((range, partitionId) -> {
            this.partitionMap.put(range, partitionId);
            this.reversePartitionMap.put(partitionId, range);
            if (partitionId >= this.nrPartitions)
            {
                this.nrPartitions = partitionId + 1;
            }
        });
        logPartitionInfo();
    }

    @Override
    public int numPartitions()
    {
        return nrPartitions;
    }

    /**
     * @param key the decorated key
     * @return the partition (non-negative) for the given key; if key is not present in the partition map, 0 is returned
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public int getPartition(Object key)
    {
        DecoratedKey decoratedKey = (DecoratedKey) key;
        Integer partition = partitionMap.get(decoratedKey.getToken());
        return partition == null ? 0 : partition;
    }

    public int numSplits()
    {
        return numberSplits;
    }

    public Range<BigInteger> getTokenRange(int partitionId)
    {
        return reversePartitionMap.get(partitionId);
    }

    private void setupTokenRangeMap(boolean randomize)
    {
        partitionMap = TreeRangeMap.create();
        reversePartitionMap = new HashMap<>();

        AtomicInteger nextPartitionId = new AtomicInteger(0);
        List<Range<BigInteger>> subRanges = tokenRangeMapping.getRangeMap()
                                                             .asMapOfRanges()
                                                             .keySet()
                                                             .stream()
                                                             .flatMap(tr -> RangeUtils.split(tr, numberSplits).stream())
                                                             .collect(Collectors.toList());
        if (randomize)
        {
            // In order to help distribute the upload load more evenly, shuffle the subranges before assigning a partition
            Collections.shuffle(subRanges);
        }
        subRanges.forEach(tr -> {
            int partitionId = nextPartitionId.getAndIncrement();

            partitionMap.put(tr, partitionId);
            reversePartitionMap.put(partitionId, tr);
        });

        this.nrPartitions = nextPartitionId.get();
    }

    // only invoked in driver
    private void validate()
    {
        validateMapSizes();
        validateCompleteRangeCoverage();
        validateRangesDoNotOverlap();
    }

    private void validateRangesDoNotOverlap()
    {
        List<Range<BigInteger>> sortedRanges = partitionMap.asMapOfRanges().keySet().stream()
                                                           .sorted(Comparator.comparing(Range::lowerEndpoint))
                                                           .collect(Collectors.toList());
        Range<BigInteger> previous = null;
        for (Range<BigInteger> current : sortedRanges)
        {
            if (previous != null)
            {
                Preconditions.checkState(!current.isConnected(previous) || current.intersection(previous).isEmpty(),
                                         "Two ranges in partition map are overlapping %s %s", previous, current);
            }

            previous = current;
        }
    }

    private void validateCompleteRangeCoverage()
    {
        RangeSet<BigInteger> missingRangeSet = TreeRangeSet.create();
        missingRangeSet.add(Range.closed(tokenRangeMapping.partitioner().minToken(),
                                         tokenRangeMapping.partitioner().maxToken()));

        partitionMap.asMapOfRanges().keySet().forEach(missingRangeSet::remove);

        List<Range<BigInteger>> missingRanges = missingRangeSet.asRanges().stream()
                                                               .filter(Range::isEmpty)
                                                               .collect(Collectors.toList());
        // noinspection unchecked
        Preconditions.checkState(missingRanges.isEmpty(),
                                 "There should be no missing ranges, but found " + missingRanges.toString());
    }

    private void validateMapSizes()
    {
        Preconditions.checkState(nrPartitions == partitionMap.asMapOfRanges().keySet().size(),
                                 String.format("Number of partitions %d not matching with partition map size %d",
                                               nrPartitions, partitionMap.asMapOfRanges().keySet().size()));
        Preconditions.checkState(nrPartitions == reversePartitionMap.keySet().size(),
                                 String.format("Number of partitions %d not matching with reverse partition map size %d",
                                               nrPartitions, reversePartitionMap.keySet().size()));
        Preconditions.checkState(nrPartitions >= tokenRangeMapping.getRangeMap().asMapOfRanges().keySet().size(),
                                 String.format("Number of partitions %d supposed to be more than number of token ranges %d",
                                               nrPartitions, tokenRangeMapping.getRangeMap().asMapOfRanges().keySet().size()));
        Preconditions.checkState(nrPartitions >= tokenRangeMapping.getTokenRanges().keySet().size(),
                                 String.format("Number of partitions %d supposed to be more than number of instances %d",
                                               nrPartitions, tokenRangeMapping.getTokenRanges().keySet().size()));
        Preconditions.checkState(partitionMap.asMapOfRanges().keySet().size() == reversePartitionMap.keySet().size(),
                                 String.format("You must be kidding me! Partition map %d and reverse map %d are not of same size",
                                               partitionMap.asMapOfRanges().keySet().size(),
                                               reversePartitionMap.keySet().size()));
    }

    // In order to best utilize the number of Spark cores while minimizing the number of commit calls,
    // we calculate the number of splits that will just match or exceed the total number of available Spark cores.
    // Note that the actual number of partitions that result from this should always be at least the number of token ranges * the number of splits,
    // but can be slightly more.
    public int calculateSplits(TokenRangeMapping<RingInstance> tokenRangeMapping,
                               Integer numberSplits,
                               int defaultParallelism,
                               Integer cores)
    {
        if (numberSplits >= 0)
        {
            return numberSplits;
        }
        int tasksToRun = Math.max(cores, defaultParallelism);
        Map<Range<BigInteger>, List<RingInstance>> rangeListMap = tokenRangeMapping.getRangeMap().asMapOfRanges();
        LOGGER.debug("Initial ranges: {}", rangeListMap);
        int ranges = rangeListMap.size();
        LOGGER.info("Number of ranges: {}", ranges);
        int calculatedSplits = divCeil(tasksToRun, ranges);
        LOGGER.info("Calculated number of splits as {}", calculatedSplits);
        return calculatedSplits;
    }

    int divCeil(int a, int b)
    {
        return (a + b - 1) / b;
    }

    /**
     * Custom serialization for Spark shuffle operations (e.g., repartitionAndSortWithinPartitions).
     * <p>
     * This method is invoked when Spark serializes the Partitioner to send it to executors during
     * shuffle operations. It saves the essential partition mappings so they can be reconstructed
     * on executors. This is separate from the broadcast variable serialization mechanism.
     * <p>
     * Note: This serialization path is used when the TokenPartitioner is passed directly to Spark
     * operations (e.g., {@code .repartitionAndSortWithinPartitions(tokenPartitioner)}), not when
     * it's broadcast as part of BulkWriterConfig.
     *
     * @param out the ObjectOutputStream to write to
     * @throws java.io.IOException if an I/O error occurs during serialization
     * @see #readObject(java.io.ObjectInputStream)
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException
    {
        out.defaultWriteObject();
        // Serialize the partition mappings
        Map<Range<BigInteger>, Integer> partitionEntries = partitionMap.asMapOfRanges();
        out.writeInt(partitionEntries.size());
        for (Map.Entry<Range<BigInteger>, Integer> entry : partitionEntries.entrySet())
        {
            out.writeObject(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    /**
     * Custom deserialization for Spark shuffle operations.
     * <p>
     * This method is invoked when Spark deserializes the Partitioner on executors during shuffle
     * operations. It reconstructs the transient fields (partitionMap, reversePartitionMap, nrPartitions)
     * from the serialized data. This ensures the Partitioner can correctly map tokens to partitions
     * after deserialization.
     * <p>
     * Note: This deserialization path is used when the TokenPartitioner was serialized by Spark
     * for shuffle operations, not when it's reconstructed from a broadcast BroadcastableTokenPartitioner.
     *
     * @param in the ObjectInputStream to read from
     * @throws java.io.IOException if an I/O error occurs during deserialization
     * @throws ClassNotFoundException if the class of a serialized object cannot be found
     * @see #writeObject(java.io.ObjectOutputStream)
     */
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        // Reconstruct partition maps
        this.partitionMap = TreeRangeMap.create();
        this.reversePartitionMap = new HashMap<>();
        this.nrPartitions = 0;

        int size = in.readInt();
        for (int i = 0; i < size; i++)
        {
            @SuppressWarnings("unchecked")
            Range<BigInteger> range = (Range<BigInteger>) in.readObject();
            int partitionId = in.readInt();

            this.partitionMap.put(range, partitionId);
            this.reversePartitionMap.put(partitionId, range);
            if (partitionId >= this.nrPartitions)
            {
                this.nrPartitions = partitionId + 1;
            }
        }
    }
}
