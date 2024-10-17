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

import org.apache.cassandra.db.rows.CellPath;

public final class CollectionElement
{
    // the path to store the value in the collection. Consider it as the key
    public final CellPath cellPath;
    // the value to be stored in the collection.
    public final Object value;

    private CollectionElement(CellPath cellPath, Object value)
    {
        this.cellPath = cellPath;
        this.value = value;
    }

    public static CollectionElement living(CellPath cellPath, Object value)
    {
        return new CollectionElement(cellPath, value);
    }

    public static CollectionElement deleted(CellPath cellPath)
    {
        return new CollectionElement(cellPath, null);
    }
}
