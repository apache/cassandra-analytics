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

package org.apache.cassandra.cdc.api;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TimeUtils;

/**
 * Optionally overridable methods for custom Cdc configuration
 */
public interface CdcOptions
{
    CdcOptions DEFAULT = new CdcOptions()
    {
    };

    /**
     * @return the data center in which CDC should run.
     */
    default String dc()
    {
        return null;
    }

    /**
     * @return Cassandra partitioner
     */
    default Partitioner partitioner()
    {
        return Partitioner.Murmur3Partitioner;
    }

    /**
     * @return true if we should serialize and attempt to persist state between micro-batches.
     */
    default boolean persistState()
    {
        return true;
    }

    /**
     * Add an optional delay between micro-batches. This is to prevent busy-polling.
     *
     * @return duration
     */
    default Duration minimumDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(1000);
    }

    /**
     * Add optional sleep when insufficient replicas available to prevent spinning between list commit log calls.
     *
     * @return duration
     */
    default Duration sleepWhenInsufficientReplicas()
    {
        return Duration.ofSeconds(1);
    }

    /**
     * @return now in microseconds
     */
    default long microsecondProvider()
    {
        return TimeUtils.nowMicros();
    }

    /**
     * @return the minimum timestamp in microseconds accepted by the Cdc state, and ignored if read in the commit log.
     */
    default long minimumTimestampMicros()
    {
        return microsecondProvider() - TimeUnit.NANOSECONDS.toMicros(maximumAge().toNanos());
    }

    /**
     * @return the maximum age of mutations accepted by the Cdc state. Anything older will be purged from the CDC state.
     */
    default Duration maximumAge()
    {
        return Duration.ofSeconds(3600);
    }

    /**
     * @return true if commit log reader should discard mutations older than maximumAge when read.
     */
    default boolean discardOldMutations()
    {
        return true;
    }

    /**
     * @return the desired CDC consistency level.
     */
    default ConsistencyLevel consistencyLevel()
    {
        return ConsistencyLevel.LOCAL_QUORUM;
    }

    /**
     * @param keyspace keyspace name
     * @return the replication factor for a given keyspace
     */
    default ReplicationFactor replicationFactor(String keyspace)
    {
        Map<String, String> options = new HashMap<>();
        options.put("class", "org.apache.cassandra.locator.SimpleStrategy");
        options.put("replication_factor", "3");
        return new ReplicationFactor(options);
    }

    /**
     * @param keyspace keyspace name
     * @return minimum number of replicas required to read from to achieve the consistency level
     */
    default int minimumReplicas(String keyspace)
    {
        return consistencyLevel()
               .blockFor(replicationFactor(keyspace), dc());
    }

    /**
     * @return max timeout to wait during Cdc shutdown.
     */
    default Duration stopTimeout()
    {
        return Duration.ofMillis(3000);
    }

    /**
     * CDC runs continuously unless interrupted or shutdown.
     *
     * @return if CDC is still running.
     */
    default boolean isRunning()
    {
        return true;
    }

    /**
     * @param batchStartNanos the start time in nanoseconds of the previous microbatch.
     * @return the delay in milliseconds
     */
    default long nextDelayMillis(long batchStartNanos)
    {
        return minimumDelayBetweenMicroBatches().toMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNanos);
    }

    /**
     * @return set maximum number of epochs or less than or equal to 0 to run indefinitely.
     */
    default int maxEpochs()
    {
        return -1;
    }

    /**
     * This property throttles CDC if the commit log is backed-up and CDC needs to catch-up.
     * Increase this value if there is a high write throughput and CDC cannot keep up.
     * Decrease this value if CDC uses too many resources during burst workloads.
     * Setting to zero or negative disables.
     *
     * @return maximum number of commit logs read per epoch per instance.
     */
    default int maxCommitLogsPerInstance()
    {
        return 4;
    }

    /**
     * @return the maximum late mutations permitted in the CdcState or 0/negative to permit unbounded size.
     */
    default int maxCdcStateSize()
    {
        return 200000; // 200000 digests is approximately 200000 * 50-60 bytes ~ 10-11MiB
    }

    /**
     * @return probability value between [0.0..1.0] indicating chance of attaching a tracking uuid to a mutation for tracing.
     * Return 0 to disable.
     */
    default Double samplingRate()
    {
        return 0.0;
    }

    /**
     * @return duration delay between schema refresh
     */
    default Duration schemaRefreshDelay()
    {
        return Duration.ofSeconds(30);
    }

    /**
     * @return current Cassandra cluster version
     */
    default CassandraVersion version()
    {
        return CassandraVersion.FOURZERO;
    }
}
