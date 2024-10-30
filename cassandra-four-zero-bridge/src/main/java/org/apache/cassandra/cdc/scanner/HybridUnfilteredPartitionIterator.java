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

package org.apache.cassandra.cdc.scanner;

import org.apache.cassandra.db.commitlog.FourZeroPartitionUpdateWrapper;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An {@link UnfilteredPartitionIterator} that is composed of partition data from different tables.
 * * Note that the {@link HybridUnfilteredPartitionIterator#metadata()} reflects the metadata of the partition read
 * * from {@link HybridUnfilteredPartitionIterator#next()}.
 */
class HybridUnfilteredPartitionIterator implements UnfilteredPartitionIterator
{
    private final CdcSortedStreamScanner cdcSortedStreamScanner;
    private FourZeroPartitionUpdateWrapper next;

    HybridUnfilteredPartitionIterator(CdcSortedStreamScanner cdcSortedStreamScanner)
    {
        this.cdcSortedStreamScanner = cdcSortedStreamScanner;
    }

    /**
     * @return the table metadata of the partition of the next CdcUpdate.
     * When the next is null, this method returns null too.
     */
    @Override
    public TableMetadata metadata()
    {
        return next == null
               ? null
               : next.partitionUpdate().metadata();
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public boolean hasNext()
    {
        if (next == null)
        {
            next = cdcSortedStreamScanner.updates.poll();
        }
        return next != null;
    }

    // Note: calling it multiple times without calling hasNext does not advance.
    // It is also assumed that hasNext is called before this method.
    @Override
    public UnfilteredRowIterator next()
    {
        PartitionUpdate update = next.partitionUpdate();
        next = null;
        return update.unfilteredIterator();
    }
}
