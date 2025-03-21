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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class DataInputStreamPlusImpl extends RebufferingInputStream
{
    private final ReadableByteChannel channel;

    public DataInputStreamPlusImpl(InputStream inputStream)
    {
        super(ByteBuffer.allocate(1 << 14)); // TODO(lantoniak): Make buffer size configurable?
        this.channel = Channels.newChannel(inputStream);
        this.buffer.limit(0);
    }

    protected void reBuffer() throws IOException
    {
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            super.close();
        }
        finally
        {
            try
            {
                FileUtils.clean(buffer);
            }
            finally
            {
                channel.close();
            }
        }
    }
}
