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

import java.io.Closeable;
import java.io.IOException;

/**
 * A rid is just a pair of ids that uniquely identifies the row and the column of a data entity.
 * Reading a Cassandra SSTable pivots the data in a way that projects all columns against the rows
 * they belong to:
 * <p>
 * Cassandra:
 * r1 | c1, c2, c3
 * r2 | c4
 * r3 | c5, c6, c7, c8
 * <p>
 * Pivoted:
 * r1 | c1
 * r1 | c2
 * r1 | c3
 * r2 | c4
 * r3 | c5
 * r3 | c6
 * r3 | c7
 * r3 | c8
 * <p>
 * During a loading operation we will extract up to a few trillion items out of SSTables, so it is of
 * high importance to reuse objects - the caller to the scanner creates a rid using the
 * callers implementation of those interfaces; the scanner then calls set**Copy() to provide the data
 * at which point the implementation should make a copy of the provided bytes.
 * <p>
 * Upon return from the next() call the current values of the scanner can be obtained by calling
 * the methods in getPartitionKey(), getColumnName(), getValue().
 *
 * @param <T> type of object returned by rid() method.
 */
@SuppressWarnings("unused")
public interface StreamScanner<T> extends Closeable
{
    /**
     * Exposes the data to be consumed.
     * <p>Implementation note: The data should always be updated to the current partition if {@code next} returns {@code true}.
     * Calls to {@code data} should be idempotent and not alter the state of the underlying scanner.
     * </p>
     *
     * @return data of type {@code <T>}
     */
    T data();

    /**
     * Indicate if there are more data available to read.
     * <p>Implementation note: {@code next} can alter the state of the underlying scanner,
     * so it should be called once and not called again until the {@code data} is consumed.
     * </p>
     *
     * @return true when the data is available to be consumed;
     * otherwise, return false to indicate the scanner has exhausted
     * @throws IOException
     */
    boolean next() throws IOException;

    /**
     * Consume the data from the next column and store to be returned by {@code data} method
     *
     * @throws IOException
     */
    void advanceToNextColumn() throws IOException;

    /**
     * @return {@code true} if the scanner has more columns to consume, {@code false} otherwise
     */
    default boolean hasMoreColumns()
    {
        return true;
    }
}
