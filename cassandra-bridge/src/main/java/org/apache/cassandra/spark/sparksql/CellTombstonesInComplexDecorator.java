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

package org.apache.cassandra.spark.sparksql;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.UTF8String;

public class CellTombstonesInComplexDecorator extends RowBuilderDecorator
{
    private final int columnPos;
    private final Map<String, ArrayData> tombstonedKeys = new LinkedHashMap<>();

    public CellTombstonesInComplexDecorator(RowBuilder delegate)
    {
        super(delegate);
        // Last item after this expansion is for the list of cell tombstones inside a complex data
        columnPos = internalExpandRow();
    }

    @Override
    protected int extraColumns()
    {
        return 1;
    }

    @Override
    public void reset()
    {
        super.reset();
        tombstonedKeys.clear();
    }

    @Override
    public void onCell(Cell cell)
    {
        super.onCell(cell);
        if (cell instanceof TombstonesInComplex)
        {
            TombstonesInComplex tombstones = (TombstonesInComplex) cell;

            Object[] keys = tombstones.tombstonedKeys.stream()
                                                           .map(ByteBuffer::array)
                                                           .toArray();
            tombstonedKeys.put(tombstones.columnName, ArrayData.toArrayData(keys));
        }
    }

    @Override
    public GenericInternalRow build()
    {
        // Append isUpdate flag
        Object[] result = array();
        if (!tombstonedKeys.isEmpty())
        {
            Object[] columns = new Object[tombstonedKeys.size()];
            Object[] tombstones = new Object[tombstonedKeys.size()];
            int index = 0;
            for (Map.Entry<String, ArrayData> entry : tombstonedKeys.entrySet())
            {
                columns[index] = UTF8String.fromString(entry.getKey());
                tombstones[index] = entry.getValue();
                index++;
            }
            result[columnPos] = ArrayBasedMapData.apply(columns, tombstones);
        }
        else
        {
            result[columnPos] = null;
        }

        return super.build();
    }
}
