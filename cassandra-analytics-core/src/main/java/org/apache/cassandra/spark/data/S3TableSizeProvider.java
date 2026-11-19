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

package org.apache.cassandra.spark.data;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.backup.BackupReader;

/**
 * Implementation of {@link TableSizeProvider} that uses S3 backup metadata to calculate the table size
 * without requiring sidecar connectivity.
 */
public class S3TableSizeProvider implements TableSizeProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3TableSizeProvider.class);

    private final BackupReader s3BackupReader;
    private final String clusterName;

    public S3TableSizeProvider(BackupReader s3BackupReader, String clusterName)
    {
        this.s3BackupReader = s3BackupReader;
        this.clusterName = clusterName;
    }

    /**
     * Returns the total used space for {@code table} across the datacenter by aggregating
     * the sizes of all SSTable files from the S3 backup.
     *
     * @param keyspace   the keyspace where the table lives
     * @param table      the table to get the size from
     * @param datacenter the datacenter
     * @return the total used space for the table across the datacenter
     */
    @Override
    public long tableSizeInBytes(String keyspace, String table, String datacenter)
    {
        try
        {
            // Get all SSTables for this table from S3
            Map<SSTableKey, Map<FileType, Long>> sstables = s3BackupReader.sstables(clusterName, keyspace, table, datacenter);

            long totalSize = 0;

            // Sum up the Data.db file sizes from all SSTables
            for (Map<FileType, Long> componentSizes : sstables.values())
            {
                Long dataFileSize = componentSizes.get(FileType.DATA);
                if (dataFileSize != null)
                {
                    totalSize += dataFileSize;
                }
            }

            LOGGER.info("Calculated S3 table size for {}.{} in datacenter {}: {} bytes from {} SSTables",
                        keyspace, table, datacenter, totalSize, sstables.size());

            return totalSize;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Error occurred while determining the S3 table size for table '%s.%s'",
                                                     keyspace, table), ex);
        }
    }
}
