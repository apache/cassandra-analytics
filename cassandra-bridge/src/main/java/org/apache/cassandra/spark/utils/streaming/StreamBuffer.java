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

import java.nio.ByteBuffer;

/**
 * A generic wrapper around bytes to allow for on/off-heap byte arrays,
 * whichever the underlying {@link SSTableSource} implementation uses
 */
public interface StreamBuffer
{
    void getBytes(int index, ByteBuffer destination, int length);

    void getBytes(int index, byte[] destination, int destinationIndex, int length);

    byte getByte(int index);

    int readableBytes();

    void release();

    static ByteArrayWrapper wrap(byte[] bytes)
    {
        return new ByteArrayWrapper(bytes);
    }

    class ByteArrayWrapper implements StreamBuffer
    {
        private final byte[] bytes;

        private ByteArrayWrapper(byte[] bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public void getBytes(int index, ByteBuffer destination, int length)
        {
            destination.put(bytes, index, length);
        }

        @Override
        public void getBytes(int index, byte[] destination, int destinationIndex, int length)
        {
            System.arraycopy(bytes, index, destination, destinationIndex, length);
        }

        @Override
        public byte getByte(int index)
        {
            return bytes[index];
        }

        @Override
        public int readableBytes()
        {
            return bytes.length;
        }

        @Override
        public void release()
        {
        }
    }
}
