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

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;

public class SchemaUpdater
{
    private SchemaUpdater()
    {
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.load(keyspaceMetadata);
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.load(keyspaceMetadata);
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, Types userTypes)
    {
        schema.load(keyspaceMetadata);
    }

    /**
     * Replaces the metadata of an existing keyspace with metadata that holds fewer tables
     */
    public static void removeTables(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.load(keyspaceMetadata);
    }

    public static void updateTable(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.load(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata)));
    }
}
