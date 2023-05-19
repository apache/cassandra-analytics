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

public class SetType<T> extends CollectionType<T, T>
{
    public final ColumnType<T> elementType;

    public SetType(ColumnType<T> elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public T parseCollectionColumn(ByteBuffer colNameSuffix, ByteBuffer colValue)
    {
        return ColumnUtil.getField(colNameSuffix, elementType);
    }

    @Override
    public List<T> finaliseCollection(List<T> entryList)
    {
        return entryList;
    }
}
