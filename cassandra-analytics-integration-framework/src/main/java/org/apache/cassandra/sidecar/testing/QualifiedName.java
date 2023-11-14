/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.testing;

import org.apache.cassandra.cql3.ColumnIdentifier;

/**
 * Simple class representing a Cassandra table, used for integration testing
 */
public class QualifiedName
{
    private final String keyspace;
    private final String table;

    public QualifiedName(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String maybeQuotedKeyspace()
    {
        return ColumnIdentifier.maybeQuote(keyspace);
    }

    public String table()
    {
        return table;
    }

    public String maybeQuotedTable()
    {
        return ColumnIdentifier.maybeQuote(table);
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", maybeQuotedKeyspace(), maybeQuotedTable());
    }
}
