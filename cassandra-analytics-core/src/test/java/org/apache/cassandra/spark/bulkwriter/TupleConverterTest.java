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

package org.apache.cassandra.spark.bulkwriter;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockListCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockMapCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockTupleCqlType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TupleConverter to ensure it handles different Row implementations correctly.
 */
public class TupleConverterTest
{
    @Test
    public void testTupleConverterWithGenericRowWithSchema()
    {
        // Create CQL tuple type: tuple<int, text>
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT), mockCqlType(SqlToCqlTypeConverter.TEXT)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a GenericRowWithSchema
        StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));
        GenericRowWithSchema row = new GenericRowWithSchema(new Object[]{42, "test"}, schema);

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(42);
        assertThat(result[1]).isEqualTo("test");
    }

    @Test
    public void testTupleConverterWithGenericRow()
    {
        // Create CQL tuple type: tuple<int, text>
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT), mockCqlType(SqlToCqlTypeConverter.TEXT)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a GenericRow (without schema)
        // This simulates the scenario where tuples inside collections don't have schemas attached
        GenericRow row = new GenericRow(new Object[]{42, "test"});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(42);
        assertThat(result[1]).isEqualTo("test");
    }

    @Test
    public void testTupleConverterWithRowFactory()
    {
        // Create CQL tuple type: tuple<int, text>
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT), mockCqlType(SqlToCqlTypeConverter.TEXT)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // RowFactory.create returns GenericRow
        Row row = RowFactory.create(42, "test");

        // Verify it's not GenericRowWithSchema
        assertThat(row).isNotInstanceOf(GenericRowWithSchema.class);
        assertThat(row).isInstanceOf(org.apache.spark.sql.Row.class);

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(42);
        assertThat(result[1]).isEqualTo("test");
    }

    @Test
    public void testCustomRowImplementation()
    {
        // Create CQL tuple type: tuple<int, text>
        CqlField.CqlTuple tupleType = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT),
                                                                     mockCqlType(SqlToCqlTypeConverter.TEXT)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a custom Row implementation to ensure converter works with any Row interface
        Row customRow = new Row()
        {
            private final Object[] values = new Object[]{99, "custom"};

            @Override
            public int length()
            {
                return values.length;
            }

            @Override
            public int size()
            {
                return values.length;
            }

            @Override
            public Object get(int i)
            {
                return values[i];
            }

            @Override
            public Row copy()
            {
                return this;
            }

            @Override
            public boolean isNullAt(int i)
            {
                return values[i] == null;
            }

            @Override
            public StructType schema()
            {
                return null;
            }
        };

        // Convert - this should work with any Row implementation
        Object[] result = converter.convertInternal(customRow);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(99);
        assertThat(result[1]).isEqualTo("custom");
    }

    @Test
    public void testTupleWithUuidAndTimestamp()
    {
        // Create CQL tuple type: tuple<uuid, timestamp>
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.UUID), mockCqlType(SqlToCqlTypeConverter.TIMESTAMP)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a row with UUID and Timestamp
        UUID uuid = UUID.randomUUID();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        GenericRow row = new GenericRow(new Object[]{uuid, timestamp});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(uuid);
        assertThat(result[1]).isEqualTo(timestamp);
    }

    @Test
    public void testListConverterWithGenericRowTuples()
    {
        // Create CQL type: list<tuple<int, text>>
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT), mockCqlType(SqlToCqlTypeConverter.TEXT)));

        CqlField.CqlCollection listType = TableSchemaTestCommon.mockListCqlType(tupleType);
        SqlToCqlTypeConverter.ListConverter<?> converter = new SqlToCqlTypeConverter.ListConverter<>(listType);

        // Create a list of GenericRow tuples (simulating tuples inside a collection)
        List<Row> tuples = Arrays.asList(new GenericRow(new Object[]{1, "first"}),
                                         new GenericRow(new Object[]{2, "second"}),
                                         new GenericRow(new Object[]{3, "third"}));

        // Convert
        List<?> result = converter.convertInternal(tuples);

        // Verify
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isInstanceOf(Object[].class);

        Object[] firstTuple = (Object[]) result.get(0);
        assertThat(firstTuple[0]).isEqualTo(1);
        assertThat(firstTuple[1]).isEqualTo("first");

        Object[] secondTuple = (Object[]) result.get(1);
        assertThat(secondTuple[0]).isEqualTo(2);
        assertThat(secondTuple[1]).isEqualTo("second");
    }

    @Test
    public void testTupleInSet()
    {
        // Create CQL type: set<tuple<int, text>>
        CqlField.CqlTuple tupleType = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT),
                                                                     mockCqlType(SqlToCqlTypeConverter.TEXT)));

        CqlField.CqlCollection setType = TableSchemaTestCommon.mockCollectionCqlType(SqlToCqlTypeConverter.SET,
                                                                                     tupleType);

        SqlToCqlTypeConverter.SetConverter<?> converter = new SqlToCqlTypeConverter.SetConverter<>(setType);

        // Create a set of GenericRow tuples
        Set<Row> tuples = new HashSet<>();
        tuples.add(new GenericRow(new Object[]{1, "first"}));
        tuples.add(new GenericRow(new Object[]{2, "second"}));
        tuples.add(new GenericRow(new Object[]{3, "third"}));

        // Convert
        Set<?> result = converter.convertInternal(tuples);

        // Verify
        assertThat(result).hasSize(3);
        // All elements should be Object[] (converted tuples)
        assertThat(result).allMatch(item -> item instanceof Object[]);
    }

    @Test
    public void testTupleWithNulls()
    {
        // Create CQL tuple type: tuple<int, text, bigint>
        CqlField.CqlTuple tupleType = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT),
                                                                     mockCqlType(SqlToCqlTypeConverter.TEXT),
                                                                     mockCqlType(SqlToCqlTypeConverter.BIGINT)));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a row with all null values
        GenericRow row = new GenericRow(new Object[]{null, null, null});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify all nulls are preserved
        assertThat(result).hasSize(3);
        assertThat(result[0]).isNull();
        assertThat(result[1]).isNull();
        assertThat(result[2]).isNull();
    }

    @Test
    public void testTupleContainingList()
    {
        // Create CQL tuple type: tuple<int, list<text>>
        CqlField.CqlCollection listType = mockListCqlType(SqlToCqlTypeConverter.TEXT);
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(
        mockCqlType(SqlToCqlTypeConverter.INT),
        listType
        )
        );

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a tuple containing a list
        List<String> textList = Arrays.asList("item1", "item2", "item3");
        GenericRow row = new GenericRow(new Object[]{42, textList});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(42);
        assertThat(result[1]).isInstanceOf(List.class);

        @SuppressWarnings("unchecked")
        List<String> resultList = (List<String>) result[1];
        assertThat(resultList).containsExactly("item1", "item2", "item3");
    }

    @Test
    public void testTupleContainingMap()
    {
        // Create CQL tuple type: tuple<int, map<text, int>>
        CqlField.CqlMap mapType = mockMapCqlType(
        mockCqlType(SqlToCqlTypeConverter.TEXT),
        mockCqlType(SqlToCqlTypeConverter.INT)
        );
        CqlField.CqlTuple tupleType = mockTupleCqlType(
        Arrays.asList(
        mockCqlType(SqlToCqlTypeConverter.TEXT),
        mapType
        )
        );

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a tuple containing a map
        Map<String, Integer> textMap = new HashMap<>();
        textMap.put("key1", 1);
        textMap.put("key2", 2);
        GenericRow row = new GenericRow(new Object[]{"tuple_field", textMap});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo("tuple_field");
        assertThat(result[1]).isInstanceOf(Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Integer> resultMap = (Map<String, Integer>) result[1];
        assertThat(resultMap).containsEntry("key1", 1);
        assertThat(resultMap).containsEntry("key2", 2);
    }

    @Test
    public void testTupleContainingSet()
    {
        // Create CQL tuple type: tuple<int, set<text>>
        CqlField.CqlCollection setType = TableSchemaTestCommon.mockCollectionCqlType(SqlToCqlTypeConverter.SET,
                                                                                     mockCqlType(SqlToCqlTypeConverter.TEXT));
        CqlField.CqlTuple tupleType = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT),
                                                                     setType));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(tupleType);

        // Create a tuple containing a set
        Set<String> textSet = new HashSet<>();
        textSet.add("value1");
        textSet.add("value2");
        textSet.add("value3");
        GenericRow row = new GenericRow(new Object[]{42, textSet});

        // Convert
        Object[] result = converter.convertInternal(row);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(42);
        assertThat(result[1]).isInstanceOf(Set.class);

        @SuppressWarnings("unchecked")
        Set<String> resultSet = (Set<String>) result[1];
        assertThat(resultSet).containsExactlyInAnyOrder("value1", "value2", "value3");
    }

    @Test
    public void testDeepNestedTuples()
    {
        // Create deeply nested tuple: tuple<int, tuple<text, tuple<int, text>>>
        CqlField.CqlTuple innermost = mockTupleCqlType(
        Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT), mockCqlType(SqlToCqlTypeConverter.TEXT)));

        CqlField.CqlTuple middle = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.TEXT),
                                                                  innermost));

        CqlField.CqlTuple outermost = mockTupleCqlType(Arrays.asList(mockCqlType(SqlToCqlTypeConverter.INT),
                                                                     middle));

        SqlToCqlTypeConverter.TupleConverter converter = new SqlToCqlTypeConverter.TupleConverter(outermost);

        // Create deeply nested GenericRow structures
        GenericRow innermostRow = new GenericRow(new Object[]{999, "deepest"});
        GenericRow middleRow = new GenericRow(new Object[]{"middle", innermostRow});
        GenericRow outermostRow = new GenericRow(new Object[]{1, middleRow});

        // Convert
        Object[] result = converter.convertInternal(outermostRow);

        // Verify
        assertThat(result).hasSize(2);
        assertThat(result[0]).isEqualTo(1);
        assertThat(result[1]).isInstanceOf(Object[].class);

        Object[] middleResult = (Object[]) result[1];
        assertThat(middleResult).hasSize(2);
        assertThat(middleResult[0]).isEqualTo("middle");
        assertThat(middleResult[1]).isInstanceOf(Object[].class);

        Object[] innermostResult = (Object[]) middleResult[1];
        assertThat(innermostResult).hasSize(2);
        assertThat(innermostResult[0]).isEqualTo(999);
        assertThat(innermostResult[1]).isEqualTo("deepest");
    }
}
