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

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class RowBuilderDecorator<T extends InternalRow> implements RowBuilder<T>
{
    protected final RowBuilder<T> delegate;

    RowBuilderDecorator(RowBuilder<T> delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public int columnsCount()
    {
        return delegate.columnsCount();
    }

    @Override
    public boolean hasRegularValueColumn()
    {
        return delegate.hasRegularValueColumn();
    }

    @Override
    public void reset()
    {
        delegate.reset();
    }

    @Override
    public boolean isFirstCell()
    {
        return delegate.isFirstCell();
    }

    @Override
    public boolean hasMoreCells()
    {
        return delegate.hasMoreCells();
    }

    @Override
    public void onCell(Cell cell)
    {
        delegate.onCell(cell);
    }

    @Override
    public void copyKeys(Cell cell)
    {
        delegate.copyKeys(cell);
    }

    @Override
    public void copyValue(Cell cell)
    {
        delegate.copyValue(cell);
    }

    @Override
    public Object[] array()
    {
        return delegate.array();
    }

    @Override
    public int expandRow(int extraColumns)
    {
        return delegate.expandRow(extraColumns + extraColumns()) + extraColumns();
    }

    @Override
    public CqlTable getCqlTable()
    {
        return delegate.getCqlTable();
    }

    /**
     * Preferred to call if the decorator is adding extra columns
     *
     * @return the index of the fist extra column
     */
    protected int internalExpandRow()
    {
        return expandRow(0) - extraColumns();
    }

    protected abstract int extraColumns();

    @Override
    public int fieldIndex(String name)
    {
        return delegate.fieldIndex(name);
    }

    @Override
    public T build()
    {
        return delegate.build();
    }
}
