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

import java.util.stream.Stream;

import org.apache.cassandra.bridge.TokenRange;
import org.jetbrains.annotations.Nullable;

public interface CommitLogProvider
{
    default Stream<CommitLog> logs()
    {
        return logs(null);
    }

    /**
     * Return a list of commit logs that should be read in the current micro-batch across a set of replicas.
     *
     * @param tokenRange optional token range that defines the range to be read from.
     *                   Method should return all replicas that overlap with the range.
     *                   A null range indicates read from the entire cluster or all available sources.
     * @return map of commit logs per Cassandra replica.
     */
    Stream<CommitLog> logs(@Nullable TokenRange tokenRange);
}
