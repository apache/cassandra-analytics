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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

import org.apache.cassandra.spark.bulkwriter.token.RangeUtils;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.spark.Partitioner;

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
        validate();  // Intentionally not keeping this in readObject(), it is enough to validate in constructor alone
        LOGGER.info("Partition map " + partitionMap);
        LOGGER.info("Reverse partition map " + reversePartitionMap);
        LOGGER.info("Number of partitions {}", nrPartitions);
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

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.defaultWriteObject();
        HashMap<Range<BigInteger>, Integer> partitionEntires = new HashMap<>();
        partitionMap.asMapOfRanges().forEach(partitionEntires::put);
        out.writeObject(partitionEntires);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException
    {
        in.defaultReadObject();
        HashMap<Range<BigInteger>, Integer> partitionEntires = (HashMap<Range<BigInteger>, Integer>) in.readObject();
        partitionMap = TreeRangeMap.create();
        reversePartitionMap = new HashMap<>();
        partitionEntires.forEach((r, i) -> {
            partitionMap.put(r, i);
            reversePartitionMap.put(i, r);
            nrPartitions++;
        });
        LOGGER.info("Partition map " + partitionMap);
        LOGGER.info("Reverse partition map " + reversePartitionMap);
        LOGGER.info("Number of partitions {}", nrPartitions);
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
        LOGGER.info("Initial ranges: {}", rangeListMap);
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
}
