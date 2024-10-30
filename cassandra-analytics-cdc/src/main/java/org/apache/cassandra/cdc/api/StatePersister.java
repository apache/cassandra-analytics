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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.state.CdcState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for persisting CDC state object and loading on start-up.
 */
public interface StatePersister
{
    StatePersister STUB = new StatePersister()
    {
        public void persist(String jobId, int partitionId, @Nullable TokenRange tokenRange, ByteBuffer buf)
        {

        }

        public List<CdcState> loadState(String jobId, int partitionId, @Nullable TokenRange tokenRange)
        {
            return Collections.singletonList(CdcState.BLANK);
        }
    };

    /**
     * Optionally persist state between micro-batches, state should be stored namespaced by the jobId,
     * partitionId and start/end tokens if RangeFilter is non-null.
     *
     * @param jobId       unique identifier for CDC streaming job.
     * @param partitionId unique identifier for this partition of the streaming job.
     * @param tokenRange  TokenRange that provides the start-end token range for this state.
     * @param buf         ByteBuffer with the serialized Iterator state.
     */
    void persist(String jobId, int partitionId, @Nullable TokenRange tokenRange, @NotNull ByteBuffer buf);


    /**
     * Load last CDC state from persistant storage after a bounce, restart or configuration change
     * and merge into single canonical state object.
     *
     * @param jobId       unique identifier for CDC streaming job.
     * @param partitionId unique identifier for this partition of the streaming job.
     * @param tokenRange  TokenRange that provides the start-end token range for this Spark partition.
     * @return canonical view of the CDC state by merging one or more CDC state objects to give canonical view.
     */
    @NotNull
    default CdcState loadCanonicalState(String jobId, int partitionId, @Nullable TokenRange tokenRange)
    {
        return loadState(jobId, partitionId, tokenRange)
               .stream()
               .reduce((first, second) -> first.merge(tokenRange, second))
               .orElse(CdcState.BLANK);
    }

    /**
     * Load last CDC state from persistant storage after a bounce, restart or configuration change.
     * <p>
     * NOTE: this method may return 1 or more CDC state objects which may occur if there is a cluster
     * topology change (expansion, shrink etc.) or the CDC partition begins from a new token range
     * so it is necessary to load CDC state for all overlaping token ranges.
     * <p>
     * NOTE: it is safe to return a CDC state object even if it partially overlaps with `tokenRange`.
     *
     * @param jobId       unique identifier for CDC streaming job.
     * @param partitionId unique identifier for this partition of the streaming job.
     * @param tokenRange  TokenRange that provides the start-end token range for this Spark partition.
     * @return list of previous CDC state objects overlapping with the tokenRange.
     */
    @NotNull
    List<CdcState> loadState(String jobId, int partitionId, @Nullable TokenRange tokenRange);
}
