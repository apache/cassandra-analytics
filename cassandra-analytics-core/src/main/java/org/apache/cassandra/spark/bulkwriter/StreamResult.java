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
import java.util.List;

import com.google.common.collect.Range;

public class StreamResult implements Serializable
{
    public final String sessionID;
    public final Range<BigInteger> tokenRange;
    public final List<StreamError> failures;
    public List<CommitResult> commitResults;  // CHECKSTYLE IGNORE: Public mutable field
    public final List<RingInstance> passed;
    public final long rowCount;
    public final long bytesWritten;

    public StreamResult(String sessionID,
                        Range<BigInteger> tokenRange,
                        List<StreamError> failures,
                        List<RingInstance> passed,
                        long rowCount,
                        long bytesWritten)
    {
        this.sessionID = sessionID;
        this.tokenRange = tokenRange;
        this.failures = failures;
        this.passed = passed;
        this.rowCount = rowCount;
        this.bytesWritten = bytesWritten;
    }

    public void setCommitResults(List<CommitResult> commitResult)
    {
        this.commitResults = commitResult;
    }

    @Override
    public String toString()
    {
        return String.format("StreamResult{sessionID='%s', tokenRange=%s, failures=%s, commitResults=%s, passed=%s, " +
                             "rowCount=%d, bytesWritten=%d}",
                             sessionID, tokenRange, failures, commitResults, passed, rowCount, bytesWritten);
    }
}
