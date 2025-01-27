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

import org.apache.cassandra.spark.data.CqlTable;

public interface RowBuilder<T>
{
    CqlTable getCqlTable();

    void reset();

    boolean isFirstCell();

    boolean hasMoreCells();

    void onCell(Cell cell);

    void copyKeys(Cell cell);

    void copyValue(Cell cell);

    Object[] array();

    int columnsCount();

    boolean hasRegularValueColumn();

    int fieldIndex(String name);

    /**
     * Expand the row with more columns. The extra columns are appended to the row.
     *
     * @param extraColumns number of columns to append
     * @return length of row before expanding
     */
    int expandRow(int extraColumns);

    T build();
}
