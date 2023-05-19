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

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import org.jetbrains.annotations.NotNull;

public class CommitError implements Serializable
{
    protected final Range<BigInteger> tokenRange;
    protected final String errMsg;

    public CommitError(@NotNull Range<BigInteger> tokenRange, @NotNull String errMsg)
    {
        Preconditions.checkNotNull(tokenRange, "CommitError created without a token range");
        Preconditions.checkNotNull(errMsg, "CommitError created without an error message");
        this.tokenRange = tokenRange;
        this.errMsg = errMsg;
    }

    @Override
    public String toString()
    {
        return String.format("CommitError{tokenRange=%s, errMsg='%s'}", tokenRange, errMsg);
    }
}
