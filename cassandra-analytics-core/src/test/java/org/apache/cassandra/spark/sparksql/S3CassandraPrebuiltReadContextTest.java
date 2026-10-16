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

package org.apache.cassandra.spark.sparksql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.S3CassandraDataLayer;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3CassandraPrebuiltReadContextTest
{
    @AfterEach
    void clearRegistry()
    {
        S3CassandraPrebuiltReadContextRegistry.clear();
    }

    @Test
    void dataSourceResolvesRegisteredReadContext()
    {
        S3CassandraDataLayer dataLayer = mock(S3CassandraDataLayer.class);
        S3CassandraPrebuiltReadContext context = new S3CassandraPrebuiltReadContext("context-1", dataLayer, null);
        S3CassandraPrebuiltReadContextRegistry.register(context);

        S3CassandraDataSource dataSource = new S3CassandraDataSource();
        CaseInsensitiveStringMap options = optionsWithContext("context-1");

        assertThat(dataSource.getDataLayer(options)).isSameAs(dataLayer);
        assertThat(dataSource.getSSTableTokenIndexBroadcast(options)).isNull();
    }

    @Test
    void dataSourceFailsFastForMissingReadContext()
    {
        S3CassandraDataSource dataSource = new S3CassandraDataSource();

        assertThatThrownBy(() -> dataSource.getDataLayer(optionsWithContext("missing")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void contextCloseUnregistersReadContext()
    {
        S3CassandraDataLayer dataLayer = mock(S3CassandraDataLayer.class);
        S3CassandraPrebuiltReadContext context = new S3CassandraPrebuiltReadContext("context-1", dataLayer, null);
        S3CassandraPrebuiltReadContextRegistry.register(context);

        context.close();

        assertThat(S3CassandraPrebuiltReadContextRegistry.get("context-1")).isNull();
        verify(dataLayer).close();
    }

    @Test
    void cassandraTableOnlyAdvertisesBatchRead()
    {
        CassandraTable table = new CassandraTable(mock(DataLayer.class), new StructType());

        // Capabilities are inherited from CassandraTable (BATCH_READ + MICRO_BATCH_READ) since this port keeps
        // the upstream value-set; the prebuild path only relies on BATCH_READ.
        assertThat(table.capabilities()).contains(TableCapability.BATCH_READ);
    }

    @Test
    void scanBuilderReadSchemaDefaultsToFullSchema()
    {
        StructType schema = new StructType();
        CassandraScanBuilder scanBuilder = new CassandraScanBuilder(mock(DataLayer.class), schema, CaseInsensitiveStringMap.empty());

        assertThat(scanBuilder.build().readSchema()).isSameAs(schema);
    }

    @Test
    void tableProviderCachesDataLayerByOptions()
    {
        S3CassandraDataLayer firstDataLayer = dataLayer("first");
        S3CassandraDataLayer secondDataLayer = dataLayer("second");
        CassandraTableProvider provider = new CassandraTableProvider()
        {
            @Override
            public DataLayer getDataLayer(CaseInsensitiveStringMap options)
            {
                return "first".equals(options.get("id")) ? firstDataLayer : secondDataLayer;
            }

            @Override
            public String shortName()
            {
                return "test";
            }
        };

        provider.inferSchema(new CaseInsensitiveStringMap(Collections.singletonMap("id", "first")));
        Map<String, String> secondOptions = new HashMap<>();
        secondOptions.put("id", "second");

        Table table = provider.getTable(new StructType(), new org.apache.spark.sql.connector.expressions.Transform[0], secondOptions);

        assertThat(table.name()).isEqualTo("ks.second");
    }

    @Test
    void tableProviderCacheKeyTreatsOptionNamesCaseInsensitively()
    {
        S3CassandraDataLayer dataLayer = dataLayer("first");
        CassandraTableProvider provider = new CassandraTableProvider()
        {
            @Override
            public DataLayer getDataLayer(CaseInsensitiveStringMap options)
            {
                return dataLayer;
            }

            @Override
            public String shortName()
            {
                return "test";
            }
        };

        provider.inferSchema(new CaseInsensitiveStringMap(Collections.singletonMap("ID", "first")));
        Map<String, String> sameOptionsDifferentCase = new HashMap<>();
        sameOptionsDifferentCase.put("id", "first");

        Table table = provider.getTable(new StructType(),
                                        new org.apache.spark.sql.connector.expressions.Transform[0],
                                        sameOptionsDifferentCase);

        assertThat(table.name()).isEqualTo("ks.first");
    }

    @Test
    void readerFactoryCreationDoesNotRequireSparkSession()
    {
        S3CassandraDataLayer dataLayer = mock(S3CassandraDataLayer.class);
        CqlTable cqlTable = mock(CqlTable.class);
        when(cqlTable.partitionKeys()).thenReturn(Collections.emptyList());
        when(dataLayer.cqlTable()).thenReturn(cqlTable);
        when(dataLayer.sstableTokenIndexEnabled()).thenReturn(true);

        CassandraScanBuilder scanBuilder = new CassandraScanBuilder(dataLayer,
                                                                    new StructType(),
                                                                    CaseInsensitiveStringMap.empty(),
                                                                    null);

        PartitionReaderFactory factory = scanBuilder.createReaderFactory();

        assertThat(factory).isNotNull();
    }

    private static CaseInsensitiveStringMap optionsWithContext(String contextId)
    {
        return new CaseInsensitiveStringMap(Collections.singletonMap(S3CassandraDataSource.READ_CONTEXT_ID_KEY, contextId));
    }

    private static S3CassandraDataLayer dataLayer(String tableName)
    {
        S3CassandraDataLayer dataLayer = mock(S3CassandraDataLayer.class);
        CqlTable cqlTable = mock(CqlTable.class);
        when(cqlTable.keyspace()).thenReturn("ks");
        when(cqlTable.table()).thenReturn(tableName);
        when(dataLayer.cqlTable()).thenReturn(cqlTable);
        when(dataLayer.structType()).thenReturn(new StructType());
        return dataLayer;
    }
}
