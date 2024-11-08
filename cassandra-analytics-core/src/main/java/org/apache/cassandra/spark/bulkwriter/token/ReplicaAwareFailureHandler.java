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

package org.apache.cassandra.spark.bulkwriter.token;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.jetbrains.annotations.Nullable;

/**
 * Handles write failures of a single cluster
 * @param <I> CassandraInstance type
 */
public abstract class ReplicaAwareFailureHandler<I extends CassandraInstance>
{
    public class FailuresPerInstance
    {
        private final Multimap<I, String> errorMessagesPerInstance;

        public FailuresPerInstance()
        {
            this.errorMessagesPerInstance = ArrayListMultimap.create();
        }

        public FailuresPerInstance(Multimap<I, String> errorMessagesPerInstance)
        {
            this.errorMessagesPerInstance = ArrayListMultimap.create(errorMessagesPerInstance);
        }

        public FailuresPerInstance copy()
        {
            return new FailuresPerInstance(this.errorMessagesPerInstance);
        }

        public Set<I> instances()
        {
            return errorMessagesPerInstance.keySet();
        }

        public void addErrorForInstance(I instance, String errorMessage)
        {
            errorMessagesPerInstance.put(instance, errorMessage);
        }

        public boolean hasError(I instance)
        {
            return errorMessagesPerInstance.containsKey(instance)
                   && !errorMessagesPerInstance.get(instance).isEmpty();
        }

        public Set<Map.Entry<I, Collection<String>>> entrySet()
        {
            return errorMessagesPerInstance.asMap().entrySet();
        }

        @Override
        public String toString()
        {
            return errorMessagesPerInstance.toString();
        }
    }

    public class ConsistencyFailurePerRange
    {
        public final Range<BigInteger> range;
        public final FailuresPerInstance failuresPerInstance;

        public ConsistencyFailurePerRange(Range<BigInteger> range, FailuresPerInstance failuresPerInstance)
        {
            this.range = range;
            this.failuresPerInstance = failuresPerInstance;
        }
    }

    /**
     * Given the number of failed instances for each token range, validates if the consistency guarantees are maintained for the job
     *
     * @param tokenRangeMapping the mapping of token ranges to a Cassandra instance
     * @param job               the job to verify
     * @param cluster           cluster info
     * @return list of failed token ranges that break consistency. This should ideally be empty for a
     * successful operation.
     */
    public abstract List<ConsistencyFailurePerRange> getFailedRanges(TokenRangeMapping<I> tokenRangeMapping,
                                                                     JobInfo job,
                                                                     ClusterInfo cluster);

    /**
     * Adds a new token range as a failed token range, with errors on given instance.
     * <p>
     * It's guaranteed that failedRangesMap has overlapping ranges for the range we are trying to insert (Check
     * constructor, we are adding complete ring first).
     * <p>
     * So the scheme is to get list of overlapping ranges first. For each overlapping range get the failure map.
     * Make a copy of the map and add new failure to this map. It's important we make the copy and not use the
     * one returned from failedRangesMap map. As our range could be overlapping partially and the map could be used
     * by other range.
     *
     * @param tokenRange the range which failed
     * @param instance   the instance on which the range failed
     * @param errMessage the error that occurred for this particular range/instance pair
     */
    public abstract void addFailure(Range<BigInteger> tokenRange, I instance, String errMessage);

    /**
     * @return the set of all failed instances
     */
    public abstract Set<I> getFailedInstances();

    /**
     * Given the number of failed instances for each token range, validates if the consistency guarantees are maintained
     * for the size of the ring and the consistency level.
     *
     * @param tokenRangeMapping the mapping of token ranges to a Cassandra instance
     * @param cl                the desired consistency level
     * @param localDC           the local datacenter
     * @param replicationFactor replication of the enclosing keyspace
     * @return list of failed token ranges that break consistency. This should ideally be empty for a
     * successful operation.
     */
    protected abstract List<ConsistencyFailurePerRange> getFailedRangesInternal(TokenRangeMapping<I> tokenRangeMapping,
                                                                                ConsistencyLevel cl,
                                                                                @Nullable String localDC,
                                                                                ReplicationFactor replicationFactor);
}
