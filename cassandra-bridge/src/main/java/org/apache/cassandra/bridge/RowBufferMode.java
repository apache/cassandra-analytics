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

/**
 * Configures how data is flushed to an SSTable
 */
public enum RowBufferMode
{
    /**
     * In this mode, Cassandra will flush an SSTable to disk once it reaches the configured BufferSizeInMB.
     * This parameter is configured by the user-configurable SSTABLE_DATA_SIZE_IN_MB WriterOption. Note:
     * This is the uncompressed size of data before being written to disk, and the actual size of an SSTable
     * can be smaller based on the compression configuration for the SSTable and how compressible the data is.
     */
    BUFFERED,

    /**
     * Cassandra expects rows in sorted order and will not flush an SSTable automatically. The size of an
     * SSTable is based on the number of rows we write to the SSTable. This parameter is configured by the
     * user-configurable BATCH_SIZE WriterOption.
     */
    UNBUFFERED
}
