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

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.CqlType;
import org.jetbrains.annotations.NotNull;

public class ColumnTypes
{
    private ColumnTypes()
    {

    }

    public static ByteBuffer buildPartitionKey(@NotNull CqlTable table, @NotNull List<String> keys)
    {
        List<AbstractType<?>> partitionKeyColumnTypes = partitionKeyColumnTypes(table);
        if (table.partitionKeys().size() == 1)
        {
            // Single partition key
            return partitionKeyColumnTypes.get(0).fromString(keys.get(0));
        }
        else
        {
            // Composite partition key
            ByteBuffer[] buffers = new ByteBuffer[keys.size()];
            for (int index = 0; index < buffers.length; index++)
            {
                buffers[index] = partitionKeyColumnTypes.get(index).fromString(keys.get(index));
            }
            return CompositeType.build(ByteBufferAccessor.instance, buffers);
        }
    }

    @VisibleForTesting
    public static List<AbstractType<?>> partitionKeyColumnTypes(@NotNull CqlTable table)
    {
        return table.partitionKeys().stream()
                    .map(CqlField::type)
                    .map(type -> (CqlType) type)
                    .map(type -> type.dataType(true))
                    .collect(Collectors.toList());
    }
}
