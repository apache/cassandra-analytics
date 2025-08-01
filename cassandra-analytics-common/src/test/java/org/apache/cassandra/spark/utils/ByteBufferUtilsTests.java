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

package org.apache.cassandra.spark.utils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("UnstableApiUsage")
public class ByteBufferUtilsTests
{
    @Test
    public void testSkipBytesFully() throws IOException
    {
        testSkipBytesFully("abc".getBytes(StandardCharsets.UTF_8));
        testSkipBytesFully("abcdefghijklm".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadRemainingBytes() throws IOException
    {
        testReadRemainingBytes("");
        testReadRemainingBytes("abc");
        testReadRemainingBytes("abcdefghijklm");
    }

    @Test
    public void testGetArray()
    {
        testGetArray("");
        testGetArray("abc");
        testGetArray("abcdefghijklm");
    }

    @Test
    public void testHexString()
    {
        // Casts to (ByteBuffer) required when compiling with Java 8
        assertThat(ByteBufferUtils.toHexString((ByteBuffer) ByteBuffer.allocate(8).putLong(500L).flip())).isEqualTo("00000000000001F4");
        assertThat(ByteBufferUtils.toHexString(ByteBuffer.wrap(new byte[]{'a', 'b', 'c'}))).isEqualTo("616263");
        assertThat(ByteBufferUtils.toHexString((ByteBuffer) ByteBuffer.allocate(8).putLong(92848484L).asReadOnlyBuffer().flip())).isEqualTo("000000000588C164");
        assertThat(ByteBufferUtils.toHexString(null)).isEqualTo("null");

        assertThat(ByteBufferUtils.toHexString(new byte[]{'a', 'b', 'c'}, 0, 3)).isEqualTo("616263");
        assertThat(ByteBufferUtils.toHexString(new byte[]{'a', 'b', 'c'}, 2, 1)).isEqualTo("63");
    }

    private static void testGetArray(String str)
    {
        assertThat(new String(ByteBufferUtils.getArray(ByteBuffer.wrap(str.getBytes())), StandardCharsets.UTF_8)).isEqualTo(str);
    }

    private static void testReadRemainingBytes(String str) throws IOException
    {
        assertThat(new String(ByteBufferUtils.readRemainingBytes(new ByteArrayInputStream(str.getBytes()), str.length()), StandardCharsets.UTF_8))
        .isEqualTo(str);
    }

    private static void testSkipBytesFully(byte[] bytes) throws IOException
    {
        int length = bytes.length;
        ByteArrayDataInput in = ByteStreams.newDataInput(bytes, 0);
        ByteBufferUtils.skipBytesFully(in, 1);
        ByteBufferUtils.skipBytesFully(in, length - 2);
        assertThat(in.readByte()).isEqualTo(bytes[length - 1]);
        assertThatThrownBy(() -> ByteBufferUtils.skipBytesFully(in, 1))
                .isInstanceOf(EOFException.class);
    }
}
