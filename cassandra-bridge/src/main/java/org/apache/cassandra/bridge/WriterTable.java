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

package org.apache.cassandra.bridge;

import org.apache.cassandra.spark.data.CqlTable;
import org.jetbrains.annotations.NotNull;

/**
 * A table that the {@link SSTableWriter} of {@link CassandraBridge#getSSTableWriter} writes, with the
 * {@link PartitionKeyEncoder} and the {@link CanonicalValueConverter} that match the writer. All three are built
 * from the same schema update, so they agree on the definition of the table.
 * <p>
 * Thread-safe.
 */
public final class WriterTable
{
    private final CqlTable table;
    private final PartitionKeyEncoder partitionKeyEncoder;
    private final CanonicalValueConverter canonicalValueConverter;

    public WriterTable(@NotNull CqlTable table,
                       @NotNull PartitionKeyEncoder partitionKeyEncoder,
                       @NotNull CanonicalValueConverter canonicalValueConverter)
    {
        this.table = table;
        this.partitionKeyEncoder = partitionKeyEncoder;
        this.canonicalValueConverter = canonicalValueConverter;
    }

    /**
     * @return the table
     */
    @NotNull
    public CqlTable table()
    {
        return table;
    }

    /**
     * @return a {@link PartitionKeyEncoder} that serializes the partition keys of the table in the same way as the
     * SSTable writer
     */
    @NotNull
    public PartitionKeyEncoder partitionKeyEncoder()
    {
        return partitionKeyEncoder;
    }

    /**
     * @return a {@link CanonicalValueConverter} for the column values of the table that the SSTable writer writes
     */
    @NotNull
    public CanonicalValueConverter canonicalValueConverter()
    {
        return canonicalValueConverter;
    }
}
