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

import java.util.BitSet;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

public class UpdatedFieldsIndicatorDecorator extends RowBuilderDecorator
{
    private final int indicatorPosition;
    private final BitSet appearedColumns;

    public UpdatedFieldsIndicatorDecorator(RowBuilder delegate)
    {
        super(delegate);
        // Last item after this expansion is for the indicator column
        this.indicatorPosition = internalExpandRow();
        this.appearedColumns = new BitSet(columnsCount());
    }

    @Override
    protected int extraColumns()
    {
        return 1;
    }

    @Override
    public void copyKeys(Cell cell)
    {
        super.copyKeys(cell);
        int length = hasRegularValueColumn() && !cell.isTombstone() ? cell.values.length - 1 : cell.values.length;
        appearedColumns.set(0, length);
    }

    @Override
    public void copyValue(Cell cell)
    {
        super.copyValue(cell);
        appearedColumns.set(cell.position);
    }

    @Override
    public void reset()
    {
        super.reset();
        appearedColumns.clear();
    }

    @Override
    public GenericInternalRow build()
    {
        // Append updated fields indicator
        Object[] result = array();
        result[indicatorPosition] = appearedColumns.toByteArray();
        return super.build();
    }
}
