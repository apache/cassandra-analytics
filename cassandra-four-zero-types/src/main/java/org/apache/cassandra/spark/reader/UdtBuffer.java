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

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Reconstructs a non-frozen UDT from its cells. A cell exists only for the fields that were written, and its path
 * holds the field position, so each value is placed at that position and fields without a cell are encoded as null.
 */
public class UdtBuffer extends AbstractComplexTypeBuffer
{
    private final ByteBuffer[] fields;

    UdtBuffer(int fieldCount)
    {
        super(fieldCount, 0);
        this.fields = new ByteBuffer[fieldCount];
    }

    /**
     * @param cell       a cell of a non-frozen UDT
     * @param fieldCount the number of fields of the UDT
     * @return the position of the field held by the cell, or -1 if the field is unknown to the UDT, e.g. added by
     * a newer ALTER TYPE
     */
    public static int fieldPosition(Cell<?> cell, int fieldCount)
    {
        int position = ByteBufferUtil.toShort(cell.path().get(0));
        return position < fieldCount ? position : -1;
    }

    @Override
    public void addCell(Cell cell)
    {
        int position = fieldPosition(cell, fields.length);
        // a field unknown to the UDT cannot be decoded, skip it
        if (position >= 0)
        {
            fields[position] = cell.buffer();
        }
    }

    @Override
    ByteBuffer build()
    {
        return pack();
    }

    @Override
    public ByteBuffer pack()
    {
        // Field count followed by every field as [length][bytes], with length -1 for null
        int size = Integer.BYTES * (fields.length + 1);
        for (ByteBuffer field : fields)
        {
            size += field == null ? 0 : field.remaining();
        }

        ByteBuffer result = ByteBuffer.allocate(size);
        result.putInt(fields.length);
        for (ByteBuffer field : fields)
        {
            if (field == null)
            {
                result.putInt(-1);
            }
            else
            {
                result.putInt(field.remaining());
                result.put(field.duplicate());
            }
        }
        // Cast to ByteBuffer required when compiling with Java 8
        return (ByteBuffer) result.flip();
    }
}
