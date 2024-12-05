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
import java.util.List;
import java.util.function.Supplier;

import org.apache.cassandra.spark.data.CqlField;
import org.jetbrains.annotations.Nullable;

public final class ByteBufferUtils
{
    private static final ThreadLocal<CharsetDecoder> UTF8_DECODER_PROVIDER = ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);
    // the static column marker used in Cassandra, see org.apache.cassandra.db.marshal.CompositeType::STATIC_MARKER
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
        return buffer.remaining() <= 0 ? EMPTY_STRING : decoderSupplier.get().decode(buffer.duplicate()).toString();
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

    public static String toHexString(CqlField.CqlType type, Object value)
    {
        ByteBuffer buf = value == null ? null : type.serialize(value);
        return ByteBufferUtils.toHexString(buf);
    }

    public static String toHexString(@Nullable ByteBuffer buffer)
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

    /**
     * Extract component at a given position from composite buffer; return null if there are not enough components
     *
     * @param buffer   composite ByteBuffer containing one or more columns.
     * @param position column position of the component to be extracted.
     * @return a new ByteBuffer that exposes the bytes for the individual column.
     */
    public static ByteBuffer extractComponent(ByteBuffer buffer, int position)
    {
        buffer = buffer.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            ByteBuffer component = readBytesWithShortLength(buffer);
            if (index == position)
            {
                return component;
            }

            buffer.get();  // Skip end-of-component
            ++index;
        }
        return null;
    }

    /**
     * Concatenates partition keys - if there are multiple - into a composite ByteBuffer.
     *
     * @param partitionKeys list of partition keys CqlField schema objects
     * @param values        value for each partition key
     * @return ByteBuffer of composite partition keys
     */
    public static ByteBuffer buildPartitionKey(List<CqlField> partitionKeys, Object... values)
    {
        if (partitionKeys.size() == 1)
        {
            // only 1 partition key
            final CqlField key = partitionKeys.get(0);
            return key.serialize(values[key.position()]);
        }

        // composite partition key
        final ByteBuffer[] buffers = partitionKeys.stream()
                                                  .map(f -> f.serialize(values[f.position()]))
                                                  .toArray(ByteBuffer[]::new);
        return build(false, buffers);
    }

    /**
     * Builds a composite ByteBuffer containing one or more column components.
     *
     * @param isStatic true if it is a static column.
     * @param buffers  array of ByteBuffers for each individual column components.
     * @return a composite ByteBuffer concatentating all column components.
     */
    public static ByteBuffer build(boolean isStatic, ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (ByteBuffer buffer : buffers)
        {
            // 2 bytes short length + data length + 1 byte for end-of-component marker
            totalLength += 2 + buffer.remaining() + 1;
        }

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        if (isStatic)
        {
            out.putShort((short) STATIC_MARKER);
        }

        for (ByteBuffer buffer : buffers)
        {
            ByteBufferUtils.writeShortLength(out, buffer.remaining());  // Short length
            out.put(buffer.duplicate());  // Data
            out.put((byte) 0);  // End-of-component marker
        }
        out.flip();
        return out;
    }

    /**
     * Split a composite ByteBuffer into the individual components.
     *
     * @param composite composite ByteBuffer e.g. containing one or more composite partition keys.
     * @param numKeys   number of keys within the composite.
     * @return array of ByteBuffers split into the individual components.
     */
    public static ByteBuffer[] split(ByteBuffer composite, int numKeys)
    {
        // Assume all components, we truncate the array at the end if necessary, but most names will be complete.
        ByteBuffer[] components = new ByteBuffer[numKeys];
        ByteBuffer buffer = composite.duplicate();
        readStatic(buffer);
        int index = 0;
        while (buffer.remaining() > 0)
        {
            components[index++] = readBytesWithShortLength(buffer);
            buffer.get();  // Skip end-of-component
        }
        return index == components.length ? components : Arrays.copyOfRange(components, 0, index);
    }

    /**
     * Read and discard static column if it exists.
     *
     * @param buffer ByteBuffer
     */
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
