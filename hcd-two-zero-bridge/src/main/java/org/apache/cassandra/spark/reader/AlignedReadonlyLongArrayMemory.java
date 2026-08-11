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

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.spark.reader.common.BigLongArray;

/**
 * Helper class to expose {@code BigLongArray} compliant with {@code Memory} API.
 */
final class AlignedReadonlyLongArrayMemory extends SafeMemory
{
    private final BigLongArray array;

    AlignedReadonlyLongArrayMemory(BigLongArray array)
    {
        super((long) array.size << 3);
        this.array = array;
    }

    public void setByte(long offset, byte b)
    {
        throw new UnsupportedOperationException();
    }

    public void setMemory(long offset, long bytes, byte b)
    {
        throw new UnsupportedOperationException();
    }

    public void setLong(long offset, long l)
    {
        throw new UnsupportedOperationException();
    }

    public void setInt(long offset, int l)
    {
        throw new UnsupportedOperationException();
    }

    public void setShort(long offset, short l)
    {
        throw new UnsupportedOperationException();
    }

    public void setBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        throw new UnsupportedOperationException();
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    public byte getByte(long offset)
    {
        throw new UnsupportedOperationException();
    }

    public long getLong(long offset)
    {
        checkBounds(offset, offset + 8);
        int chunk = Ints.checkedCast(offset / 8);
        return array.get(chunk);
    }

    public int getInt(long offset)
    {
        throw new UnsupportedOperationException();
    }

    public void getBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count)
    {
        throw new UnsupportedOperationException();
    }

    protected void checkBounds(long start, long end)
    {
        int startChunk = Ints.checkedCast(start / 8);
        int endChunk = Ints.checkedCast(end / 8);
        assert startChunk >= 0 && endChunk <= array.size && startChunk <= endChunk : "Illegal bounds [" + start + ".." + end + "); size: " + array.size;
    }

    public void put(long trgOffset, Memory memory, long srcOffset, long size)
    {
        throw new UnsupportedOperationException();
    }

    public SafeMemory copy(long newSize)
    {
        throw new UnsupportedOperationException();
    }

    public long size()
    {
        return array.size * 8L;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        else if (!(o instanceof AlignedReadonlyLongArrayMemory))
        {
            return false;
        }
        else
        {
            AlignedReadonlyLongArrayMemory b = (AlignedReadonlyLongArrayMemory) o;
            return Objects.equals(this.array, b.array);
        }
    }

    public int hashCode()
    {
        return Objects.hashCode(this.array);
    }

    public ByteBuffer asByteBuffer(long offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    public void setByteBuffer(ByteBuffer buffer, long offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    public ByteBuffer[] asByteBuffers(long offset, long length)
    {
        throw new UnsupportedOperationException();
    }

    public String toString()
    {
        return String.format("ReadonlyLongArrayMemory(%s)", array.size);
    }
}
