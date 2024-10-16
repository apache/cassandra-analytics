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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.StreamError;
import org.apache.cassandra.spark.bulkwriter.StreamResult;

/**
 * implementation of {@link StreamResult} to return results from {@link CloudStorageStreamSession} for S3_COMPAT data
 * transport option.
 */
public class CloudStorageStreamResult extends StreamResult
{
    private static final long serialVersionUID = 9096932762489827053L;
    public final Set<CreatedRestoreSlice> createdRestoreSlices;
    public final int objectCount;

    public static CloudStorageStreamResult empty(String sessionID, Range<BigInteger> tokenRange)
    {
        return new CloudStorageStreamResult(sessionID, tokenRange, new ArrayList<>(), new ArrayList<>(), new HashSet<>(), 0, 0, 0);
    }

    public CloudStorageStreamResult(String sessionID,
                                    Range<BigInteger> tokenRange,
                                    List<StreamError> failures,
                                    List<RingInstance> passed,
                                    Set<CreatedRestoreSlice> createdRestoreSlices,
                                    int objectCount,
                                    long rowCount,
                                    long bytesWritten)
    {
        super(sessionID, tokenRange, failures, passed, rowCount, bytesWritten);
        this.createdRestoreSlices = Collections.unmodifiableSet(createdRestoreSlices);
        this.objectCount = objectCount;
    }

    @Override
    public String toString()
    {
        return "StreamResult{"
               + "sessionID='" + sessionID + '\''
               + ", tokenRange=" + tokenRange
               + ", objectCount=" + objectCount
               + ", rowCount=" + rowCount
               + ", bytesWritten=" + bytesWritten
               + ", failures=" + failures
               + ", createdRestoreSlices=" + createdRestoreSlices
               + ", passed=" + passed
               + '}';
    }
}
