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

package org.apache.cassandra.cdc.sidecar;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSetFuture;
import org.apache.cassandra.bridge.TokenRange;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface defining read and write operations to Cassandra used by the `SidecarStatePersister`
 */
public interface SidecarCdcCassandraClient
{
    SidecarCdcCassandraClient STUB = new SidecarCdcCassandraClient()
    {
        @Override
        public List<ResultSetFuture> storeStateAsync(String jobId, TokenRange range, ByteBuffer buf, long timestamp)
        {
            return Collections.emptyList();
        }

        @Override
        public Stream<byte[]> loadStateForRange(String jobId, TokenRange range)
        {
            return Stream.empty();
        }
    };

    /**
     * Durably store the CDC state
     *
     * @param jobId     cdc job id
     * @param range     cdc partition token range
     * @param buf       serialized CDC state
     * @param timestamp timestamp to use in the write mutation
     * @return list of Cassandra client ResultSetFutures
     */
    List<ResultSetFuture> storeStateAsync(@Nonnull String jobId,
                                          @Nonnull TokenRange range,
                                          @Nonnull ByteBuffer buf,
                                          long timestamp);

    /**
     * @param jobId      cdc job id
     * @param tokenRange cdc partition token range
     * @return stream of one or more serialized CDC state objects that overlap with the TokenRange
     */
    Stream<byte[]> loadStateForRange(String jobId, @Nullable TokenRange tokenRange);
}
