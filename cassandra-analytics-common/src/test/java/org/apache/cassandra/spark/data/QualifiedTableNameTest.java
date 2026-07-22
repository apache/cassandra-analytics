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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QualifiedTableNameTest
{
    @Test
    void testUnquotedIdentifiersReturnedAsIs()
    {
        QualifiedTableName name = new QualifiedTableName("mykeyspace", "mytable", false);
        assertThat(name.keyspace()).isEqualTo("mykeyspace");
        assertThat(name.table()).isEqualTo("mytable");
        assertThat(name.maybeQuotedKeyspace()).isEqualTo("mykeyspace");
        assertThat(name.maybeQuotedTable()).isEqualTo("mytable");
    }

    @Test
    void testQuotedIdentifiersWrappedInDoubleQuotes()
    {
        QualifiedTableName name = new QualifiedTableName("AAMBackend", "UserBackend", true);
        assertThat(name.maybeQuotedKeyspace()).isEqualTo("\"AAMBackend\"");
        assertThat(name.maybeQuotedTable()).isEqualTo("\"UserBackend\"");
    }

    @Test
    void testRawAccessorsUnaffectedByQuoteFlag()
    {
        QualifiedTableName name = new QualifiedTableName("AAMBackend", "UserBackend", true);
        assertThat(name.keyspace()).isEqualTo("AAMBackend");
        assertThat(name.table()).isEqualTo("UserBackend");
    }

    @Test
    void testToStringQuotesBothWhenFlagSet()
    {
        QualifiedTableName name = new QualifiedTableName("AAMBackend", "UserBackend", true);
        assertThat(name.toString()).isEqualTo("\"AAMBackend\".\"UserBackend\"");
    }

    @Test
    void testToStringUnquotedWhenFlagNotSet()
    {
        QualifiedTableName name = new QualifiedTableName("mykeyspace", "mytable", false);
        assertThat(name.toString()).isEqualTo("mykeyspace.mytable");
    }

    @Test
    void testDefaultConstructorDoesNotQuote()
    {
        QualifiedTableName name = new QualifiedTableName("AAMBackend", "UserBackend");
        assertThat(name.quoteIdentifiers()).isFalse();
        assertThat(name.maybeQuotedKeyspace()).isEqualTo("AAMBackend");
        assertThat(name.maybeQuotedTable()).isEqualTo("UserBackend");
    }

    @Test
    void testReservedWordIdentifiersQuoted()
    {
        QualifiedTableName name = new QualifiedTableName("keyspace", "table", true);
        assertThat(name.maybeQuotedKeyspace()).isEqualTo("\"keyspace\"");
        assertThat(name.maybeQuotedTable()).isEqualTo("\"table\"");
    }
}
