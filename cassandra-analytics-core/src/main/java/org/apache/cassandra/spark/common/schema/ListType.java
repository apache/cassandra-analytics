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

package org.apache.cassandra.spark.common.schema;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

public class ListType<T> extends CollectionType<T, ListType.CQLListEntry<T>>
{
    public final ColumnType<T> elementType;

    public ListType(ColumnType<T> elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public CQLListEntry<T> parseCollectionColumn(ByteBuffer colNameSuffix, ByteBuffer colValue)
    {
        return new CQLListEntry<>(ColumnUtil.getField(colNameSuffix, ColumnTypes.UUID),
                                  ColumnUtil.parseSingleColumn(elementType, colValue));
    }

    @Override
    public List<T> finaliseCollection(List<CQLListEntry<T>> entryList)
    {
        Collections.sort(entryList);
        return entryList.stream().map(entry -> entry.value).collect(Collectors.toList());
    }

    public static class CQLListEntry<T> implements Comparable<CQLListEntry<T>>
    {
        private final UUID timeUUID;
        public final T value;

        @Override
        public int compareTo(@NotNull CQLListEntry that)
        {
            return this.timeUUID.compareTo(that.timeUUID);
        }

        CQLListEntry(UUID timeUUID, T value)
        {
            this.timeUUID = timeUUID;
            this.value = value;
        }
    }
}
