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

import java.io.Serializable;
import java.util.Objects;

/**
 * A key class that uniquely identifies an SSTable across various nodes.
 */
public class SSTableKey implements Serializable
{
    private static final long serialVersionUID = 20250620L;

    private final String nodeId;
    private final String keyspace;
    private final String table;
    private final String tableId;
    private final String generationId;
    private final String crc;
    private final String fileNameWithoutType;

    /**
     * Creates an SSTableKey from the parsed components of an SSTable.
     *
     * @param nodeId              the node ID where the SSTable resides
     * @param keyspace            the keyspace name
     * @param table               the table name
     * @param tableId             the table UUID
     * @param generationId        the generation ID of the SSTable
     * @param crc                 the CRC value of the SSTable
     * @param fileNameWithoutType the SSTable filename without file type suffix (e.g., "nb-123456-big")
     */
    public SSTableKey(String nodeId, String keyspace, String table, String tableId, String generationId, String crc, String fileNameWithoutType)
    {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.keyspace = Objects.requireNonNull(keyspace, "keyspace cannot be null");
        this.table = Objects.requireNonNull(table, "table cannot be null");
        this.tableId = Objects.requireNonNull(tableId, "tableId cannot be null");
        this.generationId = Objects.requireNonNull(generationId, "generationId cannot be null");
        this.crc = Objects.requireNonNull(crc, "crc cannot be null");
        this.fileNameWithoutType = Objects.requireNonNull(fileNameWithoutType, "fileNameWithoutType cannot be null");
    }

    // Getters
    public String getNodeId()
    {
        return nodeId;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public String getTableId()
    {
        return tableId;
    }

    public String getGenerationId()
    {
        return generationId;
    }

    public String getCrc()
    {
        return crc;
    }

    public String getFileNameWithoutType()
    {
        return fileNameWithoutType;
    }

    /**
     * Generates the data filename for this SSTable.
     *
     * @return the data filename in the format fileNameWithoutType-Data.db
     */
    public String getDataFileName()
    {
        return String.format("%s-%s", fileNameWithoutType, FileType.DATA.getFileSuffix());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        SSTableKey that = (SSTableKey) o;
        return Objects.equals(nodeId, that.nodeId) &&
               Objects.equals(keyspace, that.keyspace) &&
               Objects.equals(table, that.table) &&
               Objects.equals(tableId, that.tableId) &&
               Objects.equals(generationId, that.generationId) &&
               Objects.equals(crc, that.crc) &&
               Objects.equals(fileNameWithoutType, that.fileNameWithoutType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId, keyspace, table, tableId, generationId, crc, fileNameWithoutType);
    }

    @Override
    public String toString()
    {
        return String.format("SSTableKey{nodeId='%s', keyspace='%s', table='%s', tableId='%s', generationId='%s', crc='%s', fileNameWithoutType='%s'}",
                             nodeId, keyspace, table, tableId, generationId, crc, fileNameWithoutType);
    }
}
