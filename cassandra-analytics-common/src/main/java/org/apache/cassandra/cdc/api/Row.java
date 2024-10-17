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

package org.apache.cassandra.cdc.api;

import java.util.List;

import org.apache.cassandra.spark.data.CqlField;

public interface Row
{
    Object get(int pos);

    /**
     * Indicate whether the entire row is deleted
     */
    default boolean isDeleted()
    {
        return false;
    }

    /**
     * Indicate whether the row is from an INSERT statement
     */
    default boolean isInsert()
    {
        return true;
    }

    /**
     * Get the range tombstones for this partition (todo: IRow is used as a partition. Semantically, it does not fit)
     *
     * @return null if no range tombstones exist. Otherwise, return a list of range tombstones.
     */
    default List<RangeTombstoneData> rangeTombstones()
    {
        return null;
    }

    /**
     * TTL in second. 0 means no ttl.
     *
     * @return ttl
     */
    default int ttl()
    {
        return CqlField.NO_TTL;
    }
}
