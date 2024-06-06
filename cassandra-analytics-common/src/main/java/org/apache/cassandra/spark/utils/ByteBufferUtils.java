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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;

public final class ByteBufferUtils
{
    public static final ThreadLocal<CharsetDecoder> UTF8_DECODER_PROVIDER = ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);
    public static final int STATIC_MARKER = 0xFFFF;
    private static final String EMPTY_STRING = "";
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private ByteBufferUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static void skipBytesFully(DataInput in, int bytes) throws IOException
    {
        int total = 0;
        while (total < bytes)
        {
            int skipped = in.skipBytes(bytes - total);
            if (skipped == 0)
            {
                throw new EOFException("EOF after " + total + " bytes out of " + bytes);
            }
            total += skipped;
        }
    }

    public static byte[] readRemainingBytes(InputStream in, int size) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        byte[] bytes = new byte[size];
        int length;
        while ((length = in.read(bytes)) > 0)
        {
            out.write(bytes, 0, length);
        }
        return out.toByteArray();
    }

    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // Else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    /**
     * Decode ByteBuffer into String using provided CharsetDecoder.
     *
     * @param buffer          byte buffer
     * @param decoderSupplier let the user provide their own CharsetDecoder provider
     *                        e.g. using io.netty.util.concurrent.FastThreadLocal over java.lang.ThreadLocal
     * @return decoded string
     * @throws CharacterCodingException charset decoding exception
     */
    public static String string(ByteBuffer buffer, Supplier<CharsetDecoder> decoderSupplier) throws CharacterCodingException
    {
        if (buffer.remaining() <= 0)
        {
            return EMPTY_STRING;
        }
        return decoderSupplier.get().decode(buffer.duplicate()).toString();
    }

    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return string(buffer, UTF8_DECODER_PROVIDER::get);
    }

    private static String toHexString(byte[] bytes, int length)
    {
        return toHexString(bytes, 0, length);
    }

    static String toHexString(byte[] bytes, int offset, int length)
    {
        char[] hexCharacters = new char[length << 1];

        int decimalValue;
        for (int index = offset; index < offset + length; index++)
        {
            // Calculate the int value represented by the byte
            decimalValue = bytes[index] & 0xFF;
            // Retrieve hex character for 4 upper bits
            hexCharacters[(index - offset) << 1] = HEX_ARRAY[decimalValue >> 4];
            // Retrieve hex character for 4 lower bits
            hexCharacters[((index - offset) << 1) + 1] = HEX_ARRAY[decimalValue & 0xF];
        }

        return new String(hexCharacters);
    }

    public static String toHexString(ByteBuffer buffer)
    {
        if (buffer == null)
        {
            return "null";
        }

        if (buffer.isReadOnly())
        {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.slice().get(bytes);
            return ByteBufferUtils.toHexString(bytes, bytes.length);
        }

        return ByteBufferUtils.toHexString(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    }

    public static int readFully(InputStream in, byte[] bytes, int length) throws IOException
    {
        if (length < 0)
        {
            throw new IndexOutOfBoundsException();
        }

        int total = 0;
        while (total < length)
        {
            int count = in.read(bytes, total, length - total);
            if (count < 0)
            {
                break;
            }
            total += count;
        }

        return total;
    }

    // Changes buffer position
    public static ByteBuffer readBytesWithShortLength(ByteBuffer buffer)
    {
        return readBytes(buffer, readShortLength(buffer));
    }

    // Changes buffer position
    static void writeShortLength(ByteBuffer buffer, int length)
    {
        buffer.put((byte) ((length >> 8) & 0xFF));
        buffer.put((byte) (length & 0xFF));
    }

    // Doesn't change buffer position
    static int peekShortLength(ByteBuffer buffer, int position)
    {
        int length = (buffer.get(position) & 0xFF) << 8;
        return length | (buffer.get(position + 1) & 0xFF);
    }

    // Changes buffer position
    static int readShortLength(ByteBuffer buffer)
    {
        int length = (buffer.get() & 0xFF) << 8;
        return length | (buffer.get() & 0xFF);
    }

    // Changes buffer position
    @SuppressWarnings("RedundantCast")
    public static ByteBuffer readBytes(ByteBuffer buffer, int length)
    {
        ByteBuffer copy = buffer.duplicate();
        ((Buffer) copy).limit(copy.position() + length);
        ((Buffer) buffer).position(buffer.position() + length);
        return copy;
    }

    public static void skipFully(InputStream is, long length) throws IOException
    {
        long skipped = is.skip(length);
        if (skipped != length)
        {
            throw new EOFException("EOF after " + skipped + " bytes out of " + length);
        }
    }

    // Extract component position from buffer; return null if there are not enough components
    public static ByteBuffer extractComponent(ByteBuffer buffer, int position)
    {
        buffer = buffer.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            ByteBuffer c = readBytesWithShortLength(buffer);
            if (index == position)
            {
                return c;
            }

            buffer.get();  // Skip end-of-component
            ++index;
        }
        return null;
    }

    public static ByteBuffer[] split(ByteBuffer name, int numKeys)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but most names will be complete
        ByteBuffer[] l = new ByteBuffer[numKeys];
        ByteBuffer buffer = name.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            l[index++] = readBytesWithShortLength(buffer);
            buffer.get();  // Skip end-of-component
        }
        return index == l.length ? l : Arrays.copyOfRange(l, 0, index);
    }

    public static void readStatic(ByteBuffer buffer)
    {
        if (buffer.remaining() < 2)
        {
            return;
        }

        int header = peekShortLength(buffer, buffer.position());
        if ((header & 0xFFFF) != STATIC_MARKER)
        {
            return;
        }

        readShortLength(buffer);  // Skip header
    }
}
