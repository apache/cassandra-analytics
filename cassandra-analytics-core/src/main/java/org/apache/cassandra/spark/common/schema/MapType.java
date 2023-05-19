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
import java.util.List;
import java.util.Map;

public class MapType<K, V> extends CollectionType<Map.Entry<K, V>, Map.Entry<K, V>>
{
    public final ColumnType<K> keyType;
    public final ColumnType<V> valueType;

    public MapType(ColumnType<K> keyType, ColumnType<V> valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public Map.Entry<K, V> parseCollectionColumn(ByteBuffer colNameSuffix, ByteBuffer colValue)
    {
        K key = ColumnUtil.getField(colNameSuffix, keyType);
        V value = ColumnUtil.parseSingleColumn(valueType, colValue);

        return new Map.Entry<K, V>()
        {
            @Override
            public K getKey()
            {
                return key;
            }

            @Override
            public V getValue()
            {
                return value;
            }

            @Override
            public V setValue(Object value)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public List<Map.Entry<K, V>> finaliseCollection(List<Map.Entry<K, V>> entryList)
    {
        return entryList;
    }
}
