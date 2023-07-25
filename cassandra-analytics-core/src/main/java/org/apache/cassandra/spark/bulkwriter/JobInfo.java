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
import java.util.UUID;

import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.jetbrains.annotations.NotNull;

public interface JobInfo extends Serializable
{
    // Job Information API - should this really just move back to Config? Here to try to reduce the violations of the Law of Demeter more than anything else
    ConsistencyLevel getConsistencyLevel();

    String getLocalDC();

    @NotNull
    RowBufferMode getRowBufferMode();

    int getSstableDataSizeInMB();

    int getSstableBatchSize();

    int getCommitBatchSize();

    int getCommitThreadsPerInstance();

    UUID getId();

    TokenPartitioner getTokenPartitioner();

    boolean validateSSTables();

    boolean skipExtendedVerify();

    String getFullTableName();
    boolean getSkipClean();
}
