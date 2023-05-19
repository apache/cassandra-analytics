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

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

public class CommitResult implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamResult.class);

    public final String migrationId;
    protected RingInstance instance;
    public final Map<String, Range<BigInteger>> passed;
    public final Map<String, CommitError> failures;

    public CommitResult(String migrationId, RingInstance instance, Map<String, Range<BigInteger>> commitRanges)
    {
        this.migrationId = migrationId;
        this.instance = instance;
        this.passed = new HashMap<>(commitRanges);
        this.failures = new HashMap<>();
    }

    public void addFailedCommit(String uuid, @NotNull Range<BigInteger> tokenRange, @NotNull String error)
    {
        Preconditions.checkNotNull(uuid, "Adding failed commit with missing UUID");
        Preconditions.checkNotNull(tokenRange, "Adding failed commit with missing token range");
        Preconditions.checkNotNull(error, "Adding failed commit with missing error message");
        LOGGER.error("[{}]: Failed to commit {} on {}: {}", uuid, tokenRange, instance, error);
        passed.remove(uuid);
        failures.put(uuid, new CommitError(tokenRange, error));
    }

    @Override
    public String toString()
    {
        return String.format("CommitResult{migrationId='%s', instance=%s, passed=%s, failures=%s}", migrationId, instance, passed, failures);
    }
}
