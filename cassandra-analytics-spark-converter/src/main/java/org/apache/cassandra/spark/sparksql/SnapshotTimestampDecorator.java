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

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.catalyst.InternalRow;

/**
 * Decorator that appends a constant snapshot timestamp column to every output row.
 * The value is the latest autosnap epoch (in microseconds) across all nodes in the read,
 * representing the data freshness ceiling of the batch.
 *
 * @param <T> type of row returned by this builder
 */
public class SnapshotTimestampDecorator<T extends InternalRow> extends RowBuilderDecorator<T>
{
    private final int columnPosition;
    private final long snapshotTimestampMicros;

    public SnapshotTimestampDecorator(RowBuilder<T> delegate, String fieldName, long snapshotEpochSeconds)
    {
        super(delegate);
        int width = internalExpandRow();
        int fieldIndex = fieldIndex(fieldName);
        this.columnPosition = fieldIndex >= 0 ? fieldIndex : width;
        this.snapshotTimestampMicros = TimeUnit.SECONDS.toMicros(snapshotEpochSeconds);
    }

    @Override
    protected int extraColumns()
    {
        return 1;
    }

    @Override
    public T build()
    {
        Object[] result = array();
        result[columnPosition] = snapshotTimestampMicros;
        return super.build();
    }
}
