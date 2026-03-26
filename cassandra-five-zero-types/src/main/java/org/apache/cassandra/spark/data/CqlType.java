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

package org.apache.cassandra.spark.data;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;

public abstract class CqlType extends AbstractCqlType
{
    @Override
    public CassandraVersion version()
    {
        return CassandraVersion.FIVEZERO;
    }

    /**
     * Tombstone the entire complex cell, i.e. non-frozen collection and UDT
     */
    @VisibleForTesting
    public void addComplexTombstone(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                                    ColumnMetadata cd,
                                    long deletionTime)
    {
        Preconditions.checkArgument(cd.isComplex(), "The method only works with complex columns");
        rowBuilder.addComplexDeletion(cd, DeletionTime.build(deletionTime, TimeUnit.MICROSECONDS.toSeconds(deletionTime)));
    }

    public static BufferCell tombstone(ColumnMetadata column, long timestamp, long nowInSec, CellPath path)
    {
        return BufferCell.tombstone(column, timestamp, nowInSec, path);
    }

    public static BufferCell expiring(ColumnMetadata column, long timestamp, int ttl, long nowInSec, ByteBuffer value, CellPath path)
    {
        return BufferCell.expiring(column, timestamp, ttl, nowInSec, value, path);
    }
}
