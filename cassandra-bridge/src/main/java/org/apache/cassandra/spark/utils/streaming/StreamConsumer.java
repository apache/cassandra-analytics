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

package org.apache.cassandra.spark.utils.streaming;

public interface StreamConsumer
{
    /**
     * Called when {@link SSTableSource} completes a request and passes on the underlying bytes
     *
     * NOTE: This can be called multiple times after a single {@link SSTableSource#request(long, long, StreamConsumer)}
     *
     * @param buffer StreamBuffer wrapping the bytes
     */
    void onRead(StreamBuffer buffer);

    /**
     * Called when {@link SSTableSource} has finished calling onRead for the last time
     * after {@link SSTableSource#request(long, long, StreamConsumer)} was called
     *
     * NOTE: {@link StreamConsumer#onRead(StreamBuffer)} may be called zero or more times
     *       before {@link StreamConsumer#onEnd()} is called
     */
    void onEnd();

    /**
     * Called when {@link SSTableSource} fails for any reason to request the byte range
     *
     * @param throwable throwable
     */
    void onError(Throwable throwable);
}
