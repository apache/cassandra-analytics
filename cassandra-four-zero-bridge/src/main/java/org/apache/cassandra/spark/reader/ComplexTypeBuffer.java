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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;

public abstract class ComplexTypeBuffer
{
    private final List<ByteBuffer> buffers;
    private final int cellCount;
    private int length = 0;

    public ComplexTypeBuffer(int cellCount, int bufferSize)
    {
        this.cellCount = cellCount;
        this.buffers = new ArrayList<>(bufferSize);
    }

    static ComplexTypeBuffer newBuffer(AbstractType<?> type, int cellCount)
    {
        ComplexTypeBuffer buffer;
        if (type instanceof SetType)
        {
            buffer = new SetBuffer(cellCount);
        }
        else if (type instanceof ListType)
        {
            buffer = new ListBuffer(cellCount);
        }
        else if (type instanceof MapType)
        {
            buffer = new MapBuffer(cellCount);
        }
        else if (type instanceof UserType)
        {
            buffer = new UdtBuffer(cellCount);
        }
        else
        {
            throw new IllegalStateException("Unexpected type deserializing CQL Collection: " + type);
        }
        return buffer;
    }

    void addCell(Cell cell)
    {
        add(cell.buffer());  // Copy over value
    }

    void add(ByteBuffer buffer)
    {
        buffers.add(buffer);
        length += buffer.remaining();
    }

    ByteBuffer build()
    {
        ByteBuffer result = ByteBuffer.allocate(4 + (buffers.size() * 4) + length);
        result.putInt(cellCount);
        for (ByteBuffer buffer : buffers)
        {
            result.putInt(buffer.remaining());
            result.put(buffer);
        }
        // Cast to ByteBuffer required when compiling with Java 8
        return (ByteBuffer) result.flip();
    }
}
