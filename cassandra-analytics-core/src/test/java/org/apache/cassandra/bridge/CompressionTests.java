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

package org.apache.cassandra.bridge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.spark.utils.RandomUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CompressionTests extends VersionRunner
{
    public CompressionTests(CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testCompressRandom() throws IOException
    {
        // Test with random data - not highly compressible
        testCompression(RandomUtils.randomBytes(4096));
    }

    @Test
    public void testCompressUniform() throws IOException
    {
        // Test with highly compressible data
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes, (byte) 'a');
        testCompression(bytes);
    }

    private void testCompression(byte[] bytes) throws IOException
    {
        ByteBuffer compressed = bridge.compress(bytes);
        ByteBuffer uncompressed = bridge.uncompress(compressed);
        byte[] result = new byte[uncompressed.remaining()];
        uncompressed.get(result);
        assertEquals(bytes.length, result.length);
        assertArrayEquals(bytes, result);
    }
}
