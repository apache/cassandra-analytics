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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.data.CassandraTypes;

/**
 * Optional `CassandraSource` implementation that reads values from the Cassandra cluster using the standard Cassandra client.
 * This is only used for reading unfrozen lists. In Cassandra, unfrozen lists store the list index as a timeuuid
 * which is unintelligible for downstream CDC consumers.
 */
public class CassandraClientSource implements CassandraSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClientSource.class);
    private static final String READ_QUERY_FORMAT = "SELECT %s from %s.%s where %s";
    private static final int EXPIRE_AFTER_MINUTES = 60;
    private final Session session;
    private final CassandraTypes types;
    private final Cache<String, PreparedStatement> preparedStatementCache;

    public CassandraClientSource(Session session, CassandraTypes types)
    {
        this(session, types, EXPIRE_AFTER_MINUTES);
    }

    public CassandraClientSource(Session session, CassandraTypes types, int preparedStatementCacheExpireAfterMinutes)
    {
        this.session = session;
        this.types = types;
        this.preparedStatementCache = CacheBuilder
                                      .newBuilder()
                                      .expireAfterAccess(preparedStatementCacheExpireAfterMinutes, TimeUnit.MINUTES)
                                      .build();
    }

    @Override
    public List<ByteBuffer> readFromCassandra(String keyspace, String table,
                                              List<String> columnsToFetch,
                                              List<Value> primaryKeyColumns)
    {
        // Create the read query & prepare statement
        List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(primaryKeyColumns);
        String readQuery = getReadQuery(keyspace, table, columnsToFetch, primaryKeyColumnNames);
        PreparedStatement preparedStatement;
        try
        {
            preparedStatement = preparedStatementCache.get(readQuery, () -> session.prepare(readQuery));
        }
        catch (ExecutionException e)
        {
            LOGGER.error("Unable to load prepared statement for query {}", readQuery, e);
            preparedStatement = session.prepare(readQuery);
        }

        // Get primaryKey values & execute query
        BoundStatement boundStatement = preparedStatement.bind(getPrimaryKeyObjects(types, primaryKeyColumns));
        ResultSet resultSet = session.execute(boundStatement);
        if (resultSet.isExhausted())
        {
            LOGGER.error("The read query {} to C* failed", readQuery);
            return null;
        }
        Row row = resultSet.one(); // There should only be one row

        // Create list of ByteBuffer with values of columns to fetch
        List<ByteBuffer> result = new ArrayList<>();
        for (String column : columnsToFetch)
        {
            result.add(row.getBytesUnsafe(column));
        }
        return result;
    }

    @VisibleForTesting
    static String getReadQuery(String keyspace,
                               String table,
                               List<String> columnsToFetch,
                               List<String> primaryKeyColumns)
    {
        String columnsToSelect = StringUtils.join(columnsToFetch, ",");
        String primaryKeyCondition = primaryKeyColumns.stream()
                                                      .map(primaryKeyColumn -> primaryKeyColumn + " = ?")
                                                      .collect(Collectors.joining(" , "));
        return String.format(READ_QUERY_FORMAT, columnsToSelect, keyspace, table, primaryKeyCondition);
    }

    @VisibleForTesting
    static Object[] getPrimaryKeyObjects(CassandraTypes types, List<Value> primaryKeyColumns)
    {
        return primaryKeyColumns.stream()
                                .map(valueWithMetadata -> types.parseType(valueWithMetadata.columnType).deserializeToJavaType(valueWithMetadata.getValue()))
                                .toArray();
    }

    private static List<String> getPrimaryKeyColumnNames(List<Value> primaryKeyColumns)
    {
        return primaryKeyColumns.stream()
                                .map(valueWithMetadata -> valueWithMetadata.columnName)
                                .collect(Collectors.toList());
    }

    private static class MtlsAuthProvider implements AuthProvider
    {
        @Override
        public Authenticator newAuthenticator(InetSocketAddress inetSocketAddress, String s) throws AuthenticationException
        {
            return new MutualTLSAuthenticator();
        }

        private static class MutualTLSAuthenticator implements Authenticator
        {
            public byte[] initialResponse()
            {
                return new byte[]{0, 0};
            }

            public byte[] evaluateChallenge(byte[] bytes)
            {
                return null;
            }

            public void onAuthenticationSuccess(byte[] bytes)
            {
                LOGGER.info("Successfully authenticated with mTLS");
            }
        }
    }
}
