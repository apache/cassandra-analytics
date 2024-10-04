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

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/**
 * Wrap a builder to append last modified timestamp
 */
public class LastModifiedTimestampDecorator extends RowBuilderDecorator
{
    private final int lmtColumnPosition;
    private long lastModified = 0L;

    public LastModifiedTimestampDecorator(RowBuilder delegate, String fieldName)
    {
        super(delegate);
        int width = internalExpandRow();
        int fieldIndex = fieldIndex(fieldName);
        // Determine the last modified timestamp column position based on the query
        lmtColumnPosition = fieldIndex >= 0 ? fieldIndex : width;
    }

    @Override
    public void reset()
    {
        super.reset();
        // Reset the lastModified the builder is re-used across rows
        lastModified = 0L;
    }

    @Override
    public void onCell(Cell cell)
    {
        super.onCell(cell);
        lastModified = Math.max(lastModified, cell.timestamp);
    }

    @Override
    protected int extraColumns()
    {
        return 1;
    }

    @Override
    public GenericInternalRow build()
    {
        // Append last modified timestamp
        Object[] result = array();
        result[lmtColumnPosition] = lastModified;
        return super.build();
    }
}
