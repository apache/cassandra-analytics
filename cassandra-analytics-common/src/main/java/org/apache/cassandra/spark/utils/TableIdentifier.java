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

package org.apache.cassandra.spark.utils;

import java.util.Objects;

/**
 * Qualified table name in Cassandra
 */
public class TableIdentifier
{
    private final String keyspace;
    private final String table;

    public TableIdentifier(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    public static TableIdentifier of(String keyspace, String table)
    {
        return new TableIdentifier(keyspace, table);
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }

        TableIdentifier that = (TableIdentifier) other;
        return Objects.equals(this.keyspace, that.keyspace)
            && Objects.equals(this.table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, table);
    }

    @Override
    public String toString()
    {
        return String.format("TableIdentifier{keyspace='%s', table='%s'}", keyspace, table);
    }
}
