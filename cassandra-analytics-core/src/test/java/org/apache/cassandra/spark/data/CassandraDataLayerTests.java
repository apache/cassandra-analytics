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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.spark.sql.types.DataTypes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraDataLayerTests
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS = ImmutableMap.of(
    "keyspace", "big-data",
    "table", "customers",
    "sidecar_contact_points", "localhost");

    @Test
    void testDefaultClearSnapshotStrategy()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        ClientConfig clientConfig = ClientConfig.create(options);
        assertThat(clientConfig.keyspace()).isEqualTo("big-data");
        assertThat(clientConfig.table()).isEqualTo("customers");
        assertThat(clientConfig.sidecarContactPoints()).isEqualTo("localhost");
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = clientConfig.clearSnapshotStrategy();
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo("2d");
    }

    @ParameterizedTest
    @CsvSource({"false, NOOP", "true,ONCOMPLETIONORTTL 2d"})
    void testClearSnapshotOptionSupport(Boolean clearSnapshot, String expectedClearSnapshotStrategyOption)
    {
        Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("clearsnapshot", clearSnapshot.toString());
        ClientConfig clientConfig = ClientConfig.create(options);
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy = clientConfig.clearSnapshotStrategy();
        ClientConfig.ClearSnapshotStrategy expectedClearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(false, false, expectedClearSnapshotStrategyOption);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion())
        .isEqualTo(expectedClearSnapshotStrategy.shouldClearOnCompletion());
        assertThat(clearSnapshotStrategy.hasTTL()).isEqualTo(expectedClearSnapshotStrategy.hasTTL());
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedClearSnapshotStrategy.ttl());
    }

    @Test
    void testRejectSchemaFeatureFieldConflictingWithTableColumn()
    {
        CqlTable table = mock(CqlTable.class);
        CqlField column = mock(CqlField.class);
        SchemaFeature feature = mock(SchemaFeature.class);
        SparkSqlTypeConverter typeConverter = mock(SparkSqlTypeConverter.class);
        DataLayer dataLayer = mock(DataLayer.class, CALLS_REAL_METHODS);

        when(table.fields()).thenReturn(Collections.singletonList(column));

        when(column.name()).thenReturn("column1");
        when(column.cqlTypeName()).thenReturn("text");

        when(typeConverter.sparkSqlType(eq(column), any(BigNumberConfig.class))).thenReturn(DataTypes.StringType);

        when(feature.fieldName()).thenReturn("column1");

        doReturn(table).when(dataLayer).cqlTable();
        doReturn(typeConverter).when(dataLayer).typeConverter();
        doReturn(Collections.singletonList(feature)).when(dataLayer).requestedFeatures();

        assertThatThrownBy(dataLayer::structType).isInstanceOf(IllegalArgumentException.class)
                                                 .hasMessage("Schema feature field 'column1' conflicts with an existing field");
    }

    @Test
    void testRejectDuplicateSchemaFeatureFields()
    {
        CqlTable table = mock(CqlTable.class);
        SchemaFeature ttlFeature = mock(SchemaFeature.class);
        SchemaFeature timestampFeature = mock(SchemaFeature.class);
        DataLayer dataLayer = mock(DataLayer.class, CALLS_REAL_METHODS);

        when(table.fields()).thenReturn(Collections.emptyList());

        when(ttlFeature.fieldName()).thenReturn("column1");
        when(ttlFeature.field()).thenReturn(DataTypes.createStructField("column1", DataTypes.IntegerType, true));

        when(timestampFeature.fieldName()).thenReturn("column1");

        doReturn(table).when(dataLayer).cqlTable();
        doReturn(Arrays.asList(ttlFeature, timestampFeature)).when(dataLayer).requestedFeatures();

        assertThatThrownBy(dataLayer::structType).isInstanceOf(IllegalArgumentException.class)
                                                 .hasMessage("Schema feature field 'column1' conflicts with an existing field");
    }
}
