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

package org.apache.cassandra.cdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.bridge.CassandraTypesImplementation;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.data.CassandraTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraClientSourceTest
{
    private static final CassandraTypes TYPES = new CassandraTypesImplementation();

    @Test
    public void testGetReadQuery()
    {
        String query = CassandraClientSource.getReadQuery("testKeyspace", "testTable", Arrays.asList("a", "b"), Arrays.asList("c", "d", "e"));
        String expectedQuery = "SELECT a,b from testKeyspace.testTable where c = ? , d = ? , e = ?";
        assertEquals(expectedQuery, query);
    }

    @Test
    public void testGetPrimaryKeyObjects()
    {
        List<Value> primaryKeyColumns = Arrays.asList(
        new TestValue("ks", "a", "int", TYPES.aInt().serialize(1)),
        new TestValue("ks", "b", "text", ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8)))
        );
        Object[] primaryKeyValues = CassandraClientSource.getPrimaryKeyObjects(TYPES, primaryKeyColumns);
        assertEquals(1, primaryKeyValues[0]);
        assertEquals("test", primaryKeyValues[1]);
    }

    @Test
    public void testReadFromCassandra()
    {
        CassandraClientSource cassandraSource = new CassandraClientSource(getMockSession(), TYPES);

        // column with name "a" of type int with value 1
        List<Value> primaryKeyColumns = Collections.singletonList(new TestValue("ks", "a", "int", TYPES.aInt().serialize(1)));
        List<ByteBuffer> result = cassandraSource.readFromCassandra("testKeyspace", "testTable", Collections.singletonList("b"), primaryKeyColumns);
        assertEquals(100, result.get(0).getInt(result.get(0).position()));
    }

    // mock session that returns 100 on execution of query
    private static Session getMockSession()
    {
        Session session = mock(Session.class);
        // mock row
        Row row = mock(Row.class);
        when(row.getBytesUnsafe(anyString())).thenReturn(TYPES.aInt().serialize(100));

        // mock result set
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenAnswer(invocation -> row);

        // mock prepared statement
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.bind(any())).thenAnswer(invocation -> mock(BoundStatement.class));

        // mock session
        when(session.prepare(anyString())).then(invocation -> preparedStatement);
        when(session.execute(any(Statement.class))).thenAnswer(invocation -> resultSet);
        doNothing().when(session).close();
        return session;
    }

    private static class TestValue extends Value
    {
        private TestValue(String keyspace, String columnName, String columnType, ByteBuffer value)
        {
            super(keyspace, columnName, columnType, value);
        }
    }
}
