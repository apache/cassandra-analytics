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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.bridge.SSTableSummary;

/**
 * Collect SSTables from listing the included directories
 */
public interface SSTableCollector
{
    /**
     * Include sstables under the directory
     * @param dir directory that contains sstables
     */
    void includeDirectory(Path dir);

    /**
     * Include the sstable components of an individual SSTable
     * @param sstableComponents sstable components
     */
    void includeSSTable(List<Path> sstableComponents);

    /**
     * @return total size of all sstables included
     */
    long totalSize();

    /**
     * Get an SSTable from the collector, but do not remove it
     * @return sstable or null if the collector is empty
     */
    SSTableFilesAndRange peek();

    /**
     * Get an SSTable from the collector and remove it
     * @return sstable or null if the collector is empty
     */
    SSTableFilesAndRange consumeOne();

    /**
     * @return true if the collector is empty; otherwise, false
     */
    boolean isEmpty();

    /**
     * Simple record class containing SSTable component file paths, summary and size
     */
    class SSTableFilesAndRange
    {
        public final Set<Path> files; // immutable set
        public final SSTableSummary summary;
        public final long size;

        public SSTableFilesAndRange(SSTableSummary summary, List<Path> components, long size)
        {
            this.summary = summary;
            this.files = Collections.unmodifiableSet(new HashSet<>(components));
            this.size = size;
        }
    }
}
