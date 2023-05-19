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

import java.util.List;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RangeTombstoneMarker
{
    int TOTAL_FIELDS = 4;
    int START_FIELD_POSITION = 0;
    int START_INCLUSIVE_FIELD_POSITION = 1;
    int END_FIELD_POSITION = 2;
    int END_INCLUSIVE_FIELD_POSITION = 3;

    boolean isBoundary();
    boolean isOpen(boolean value);
    boolean isClose(boolean value);

    long openDeletionTime(boolean value);
    long closeDeletionTime(boolean value);

    @Nullable
    Object[] computeRange(@Nullable Object[] range,
                          @NotNull List<InternalRow> list,
                          @NotNull CqlTable table);
}
