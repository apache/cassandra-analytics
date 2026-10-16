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
 * Stable SSTable identifier for token-index lookups.
 *
 * This intentionally copies the value fields out of {@link SSTableKey} so index lookups never depend
 * on Java object identity after Spark task and broadcast deserialization.
 */
public final class SSTableIndexKey implements Serializable
{
    private static final long serialVersionUID = 2026042501L;

    private final String nodeId;
    private final String keyspace;
    private final String table;
    private final String tableId;
    private final String generationId;
    private final String crc;
    private final String fileNameWithoutType;

    private SSTableIndexKey(String nodeId,
                            String keyspace,
                            String table,
                            String tableId,
                            String generationId,
                            String crc,
                            String fileNameWithoutType)
    {
        this.nodeId = nodeId;
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.generationId = generationId;
        this.crc = crc;
        this.fileNameWithoutType = fileNameWithoutType;
    }

    public static SSTableIndexKey from(SSTableKey key)
    {
        return new SSTableIndexKey(key.getNodeId(),
                                   key.getKeyspace(),
                                   key.getTable(),
                                   key.getTableId(),
                                   key.getGenerationId(),
                                   key.getCrc(),
                                   key.getFileNameWithoutType());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        SSTableIndexKey that = (SSTableIndexKey) obj;
        return Objects.equals(nodeId, that.nodeId)
               && Objects.equals(keyspace, that.keyspace)
               && Objects.equals(table, that.table)
               && Objects.equals(tableId, that.tableId)
               && Objects.equals(generationId, that.generationId)
               && Objects.equals(crc, that.crc)
               && Objects.equals(fileNameWithoutType, that.fileNameWithoutType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId, keyspace, table, tableId, generationId, crc, fileNameWithoutType);
    }
}
