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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;

public class RangeTombstoneDecorator extends RowBuilderDecorator
{
    private final int columnPosition;
    private final List<InternalRow> rangeTombstoneList;

    public RangeTombstoneDecorator(RowBuilder delegate)
    {
        super(delegate);
        // Last item after this expansion is for the list of cell tombstones inside a complex data
        this.columnPosition = internalExpandRow();
        this.rangeTombstoneList = new ArrayList<>();
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
        rangeTombstoneList.clear();
    }

    @Override
    public void onCell(Cell cell)
    {
        super.onCell(cell);

        if (cell instanceof RangeTombstone)
        {
            RangeTombstone rangeTombstone = (RangeTombstone) cell;
            // See SchemaFeatureSet.RANGE_DELETION#fieldDataType for the schema;
            // each range has 4 fields: Start, StartInclusive, End, EndInclusive
            Object[] range = null;
            for (RangeTombstoneMarker marker : rangeTombstone.rangeTombstoneMarkers)
            {
                // `range` array is used to collect data from `marker` and is added into `rangeTombstoneList`
                range = marker.computeRange(range, rangeTombstoneList, getCqlTable());
            }
            Preconditions.checkState(range == null, "Tombstone range should be closed");
        }
   }

    @Override
    public GenericInternalRow build()
    {
        // `result` array is declared in the `RowBuilder`, the decorator first expands the array,
        // and adds the data to its allocated slot on build, `result` array is the data to produce a Spark row
        Object[] result = array();
        if (rangeTombstoneList.isEmpty())
        {
            result[columnPosition] = null;
        }
        else
        {
            result[columnPosition] = ArrayData.toArrayData(rangeTombstoneList.toArray());
        }

        return super.build();
    }
}
