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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Range;

import org.jetbrains.annotations.NotNull;

/**
 * Broadcastable wrapper for TokenPartitioner with ZERO transient fields to optimize Spark broadcasting.
 * <p>
 * Only contains the partition mappings; executors will use this to reconstruct TokenPartitioner.
 * <p>
 * <b>Why ZERO transient fields matters:</b><br>
 * Spark's {@link org.apache.spark.util.SizeEstimator} uses reflection to estimate object sizes before broadcasting.
 * Each transient field forces SizeEstimator to inspect the field's type hierarchy, which is expensive.
 * Logger references are particularly costly due to their deep object graphs (appenders, layouts, contexts).
 * By eliminating ALL transient fields and Logger references, we:
 * <ul>
 *   <li>Minimize SizeEstimator reflection overhead during broadcast preparation</li>
 *   <li>Reduce broadcast variable serialization size</li>
 *   <li>Avoid accidental serialization of non-serializable objects</li>
 * </ul>
 */
public final class BroadcastableTokenPartitioner implements Serializable
{
    private static final long serialVersionUID = -8787074052066841748L;

    // Essential fields broadcast to executors - the partition mappings
    private final Map<Range<BigInteger>, Integer> partitionEntries;
    private final Integer numberSplits;

    /**
     * Creates a BroadcastableTokenPartitioner from a TokenPartitioner.
     * Extracts only the partition mappings, avoiding the Logger.
     *
     * @param source the source TokenPartitioner
     */
    public static BroadcastableTokenPartitioner from(@NotNull TokenPartitioner source)
    {
        // Extract partition mappings - these are already computed and won't change
        Map<Range<BigInteger>, Integer> partitionEntries = new HashMap<>();
        for (int i = 0; i < source.numPartitions(); i++)
        {
            Range<BigInteger> range = source.getTokenRange(i);
            if (range != null)
            {
                partitionEntries.put(range, i);
            }
        }

        return new BroadcastableTokenPartitioner(partitionEntries, source.numSplits());
    }

    private BroadcastableTokenPartitioner(Map<Range<BigInteger>, Integer> partitionEntries,
                                         Integer numberSplits)
    {
        this.partitionEntries = partitionEntries;
        this.numberSplits = numberSplits;
    }

    public Map<Range<BigInteger>, Integer> getPartitionEntries()
    {
        return partitionEntries;
    }

    public Integer numSplits()
    {
        return numberSplits;
    }
}
