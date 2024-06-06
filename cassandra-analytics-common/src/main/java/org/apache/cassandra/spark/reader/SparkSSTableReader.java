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

package org.apache.cassandra.spark.reader;

import java.math.BigInteger;

import org.apache.cassandra.bridge.TokenRange;

public interface SparkSSTableReader
{
    BigInteger firstToken();

    BigInteger lastToken();

    default TokenRange range()
    {
        // It is possible for the range boundaries to be inverted in unsorted SSTables produced by
        // the writer of Cassandra version 3.0, at least this is what happens when running unit tests
        return firstToken().compareTo(lastToken()) <= 0 ? TokenRange.closed(firstToken(), lastToken())
                                                        : TokenRange.closed(lastToken(), firstToken());
    }

    /**
     * @return true if this SSTable should not be read as part of this Spark partition
     */
    boolean ignore();

    /**
     * @param reader SSTable reader
     * @param range  token range
     * @return true if SSTable reader overlaps with a given token range
     */
    static boolean overlaps(SparkSSTableReader reader, TokenRange range)
    {
        return range.isConnected(reader.range());
    }
}
