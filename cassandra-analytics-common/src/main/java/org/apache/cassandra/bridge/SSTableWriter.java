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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface SSTableWriter extends Closeable
{
    /**
     * Write a new row
     *
     * @param values values of the row
     * @throws IOException i/o exception when writing
     */
    void addRow(Map<String, Object> values) throws IOException;

    /**
     * Register the listener for the set of newly produced sstable, identified by its unique base filename.
     * The base filename is filename of sstable without the component suffix.
     * For example, "nb-1-big"
     * <p>
     * Note that once a produced sstable has been returned, the returning lists of the subsequent calls do not include it anymore.
     * Therefore, it only returns the _newly_ produced sstables.
     */
    //TODO - create SSTableDescriptor
    void setSSTablesProducedListener(Consumer<Set<String>> listener);
}
