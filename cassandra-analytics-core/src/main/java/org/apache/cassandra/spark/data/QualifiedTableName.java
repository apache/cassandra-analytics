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
package org.apache.cassandra.spark.data;

import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * Contains the keyspace and table name in Cassandra
 */
public class QualifiedTableName
{
    @NotNull
    private final String keyspace;
    @NotNull
    private final String table;
    private final boolean quoteIdentifiers;

    /**
     * Constructs a qualified table name with the given {@code keyspace} and {@code tableName}
     *
     * @param keyspace  the unquoted keyspace name in Cassandra
     * @param tableName the unquoted table name in Cassandra
     * @param quoteIdentifiers indicate whether the identifiers should be quoted
     */
    public QualifiedTableName(String keyspace, String tableName, boolean quoteIdentifiers)
    {
        this.keyspace = Objects.requireNonNull(keyspace);
        this.table = Objects.requireNonNull(tableName);
        this.quoteIdentifiers = quoteIdentifiers;
    }

    /**
     * Construct a qualified table name that its keyspace and table name does not need to be quoted
     *
     * @param keyspace  the unquoted keyspace name in Cassandra
     * @param tableName the unquoted table name in Cassandra
     */
    public QualifiedTableName(String keyspace, String tableName)
    {
        this(keyspace, tableName, false);
    }

    /**
     * @return the keyspace in Cassandra
     */
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * @return the table name in Cassandra
     */
    public String table()
    {
        return table;
    }

    /**
     * @return the identifiers should be quoted
     */
    public boolean quoteIdentifiers()
    {
        return quoteIdentifiers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return maybeQuote(keyspace) + "." + maybeQuote(table);
    }

    private String maybeQuote(String name)
    {
        if (quoteIdentifiers)
        {
            return '"' + name + '"';
        }

        return name;
    }
}
