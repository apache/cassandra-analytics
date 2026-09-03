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

import java.util.function.Function;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Wrapper allowing to append any cell attribute to Spark row.
 * @param <T> type of row returned by this builder
 */
public class CellMetadataDecorator<T extends InternalRow> extends RowBuilderDecorator<T>
{
    private final int sourceColumnPosition;
    private final int metadataColumnPosition;
    private final Function<Cell, Object> metadataGetter;
    private Object metadata;

    public CellMetadataDecorator(RowBuilder<T> delegate,
                                 int sourceColumnPosition,
                                 String fieldName,
                                 Function<Cell, Object> metadataGetter)
    {
        super(delegate);
        this.sourceColumnPosition = sourceColumnPosition;
        this.metadataGetter = metadataGetter;

        int width = internalExpandRow();
        int fieldIndex = fieldIndex(fieldName);
        this.metadataColumnPosition = fieldIndex >= 0 ? fieldIndex : width;
    }

    @Override
    public void reset()
    {
        super.reset();
        metadata = null;
    }

    @Override
    public void onCell(Cell cell)
    {
        super.onCell(cell);
        if (cell.isPkCkOnly || cell.position != sourceColumnPosition)
        {
            return;
        }
        // apply metadata only to non-primary key columns only
        metadata = metadataGetter.apply(cell);
    }

    @Override
    protected int extraColumns()
    {
        return 1;
    }

    @Override
    public T build()
    {
        array()[metadataColumnPosition] = metadata;
        return super.build();
    }
}
