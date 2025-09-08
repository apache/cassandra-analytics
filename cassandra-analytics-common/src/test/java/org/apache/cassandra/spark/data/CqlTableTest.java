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
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for CqlTable, especially focusing on findTuple method
 * which handles tuples in various nested structures.
 */
class CqlTableTest
{
    @Test
    void testFindTupleDirectTuple()
    {
        // Test: direct tuple field
        CqlField.CqlTuple tupleType = CqlField.CqlTuple.builder()
                                                       .withType(CqlField.CqlType.ascii())
                                                       .withType(CqlField.CqlType.cint())
                                                       .build();
        
        CqlField tupleField = CqlField.builder("tuple_col", tupleType)
                                      .isValueColumn(true)
                                      .build();
        
        List<CqlField> fields = Collections.singletonList(tupleField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("tuple_col");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleFrozenTuple()
    {
        // Test: frozen<tuple<...>>
        CqlField.CqlTuple innerTuple = CqlField.CqlTuple.builder()
                                                        .withType(CqlField.CqlType.ascii())
                                                        .withType(CqlField.CqlType.cint())
                                                        .build();
        
        CqlField.CqlFrozen frozenTuple = CqlField.CqlType.frozen(innerTuple);
        
        CqlField frozenTupleField = CqlField.builder("frozen_tuple_col", frozenTuple)
                                            .isValueColumn(true)
                                            .build();
        
        List<CqlField> fields = Collections.singletonList(frozenTupleField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("frozen_tuple_col");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInList()
    {
        // Test: list<tuple<int, text>>
        CqlField.CqlTuple tupleType = CqlField.CqlTuple.builder()
                                                       .withType(CqlField.CqlType.cint())
                                                       .withType(CqlField.CqlType.text())
                                                       .build();
        
        CqlField.CqlList listOfTuples = CqlField.CqlType.list(tupleType);
        
        CqlField listField = CqlField.builder("tuple_list", listOfTuples)
                                     .isValueColumn(true)
                                     .build();
        
        List<CqlField> fields = Collections.singletonList(listField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("tuple_list");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInSet()
    {
        // Test: set<tuple<int, text>>
        CqlField.CqlTuple tupleType = CqlField.CqlTuple.builder()
                                                       .withType(CqlField.CqlType.cint())
                                                       .withType(CqlField.CqlType.text())
                                                       .build();
        
        CqlField.CqlSet setOfTuples = CqlField.CqlType.set(tupleType);
        
        CqlField setField = CqlField.builder("tuple_set", setOfTuples)
                                    .isValueColumn(true)
                                    .build();
        
        List<CqlField> fields = Collections.singletonList(setField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("tuple_set");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInMapKey()
    {
        // Test: map<tuple<int, text>, text>
        CqlField.CqlTuple keyTuple = CqlField.CqlTuple.builder()
                                                      .withType(CqlField.CqlType.cint())
                                                      .withType(CqlField.CqlType.text())
                                                      .build();
        
        CqlField.CqlMap mapWithTupleKey = CqlField.CqlType.map(keyTuple, CqlField.CqlType.text());
        
        CqlField mapField = CqlField.builder("map_tuple_key", mapWithTupleKey)
                                    .isValueColumn(true)
                                    .build();
        
        List<CqlField> fields = Collections.singletonList(mapField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("map_tuple_key");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInMapValue()
    {
        // Test: map<text, tuple<int, text>>
        CqlField.CqlTuple valueTuple = CqlField.CqlTuple.builder()
                                                        .withType(CqlField.CqlType.cint())
                                                        .withType(CqlField.CqlType.text())
                                                        .build();
        
        CqlField.CqlMap mapWithTupleValue = CqlField.CqlType.map(CqlField.CqlType.text(), valueTuple);
        
        CqlField mapField = CqlField.builder("map_tuple_value", mapWithTupleValue)
                                    .isValueColumn(true)
                                    .build();
        
        List<CqlField> fields = Collections.singletonList(mapField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("map_tuple_value");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInFrozenList()
    {
        // Test: frozen<list<tuple<int, text>>>
        CqlField.CqlTuple tupleType = CqlField.CqlTuple.builder()
                                                       .withType(CqlField.CqlType.cint())
                                                       .withType(CqlField.CqlType.text())
                                                       .build();
        
        CqlField.CqlList listOfTuples = CqlField.CqlType.list(tupleType);
        CqlField.CqlFrozen frozenList = CqlField.CqlType.frozen(listOfTuples);
        
        CqlField frozenListField = CqlField.builder("frozen_tuple_list", frozenList)
                                           .isValueColumn(true)
                                           .build();
        
        List<CqlField> fields = Collections.singletonList(frozenListField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("frozen_tuple_list");
        assertNotNull(result);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void testFindTupleInNestedMap()
    {
        // Test: map<int, map<text, tuple<int, text>>>
        CqlField.CqlTuple innerTuple = CqlField.CqlTuple.builder()
                                                        .withType(CqlField.CqlType.cint())
                                                        .withType(CqlField.CqlType.text())
                                                        .build();
        
        CqlField.CqlMap innerMap = CqlField.CqlType.map(CqlField.CqlType.text(), innerTuple);
        CqlField.CqlMap outerMap = CqlField.CqlType.map(CqlField.CqlType.cint(), innerMap);
        
        CqlField nestedMapField = CqlField.builder("nested_map", outerMap)
                                          .isValueColumn(true)
                                          .build();
        
        List<CqlField> fields = Collections.singletonList(nestedMapField);
        CqlTable table = createTestTable(fields);
        
        // Currently our implementation finds the first level, not nested deeper
        // This is expected behavior for now - we find tuple at first collection level
        CqlField.CqlTuple result = table.findTuple("nested_map");
        assertNull(result); // outer map key is int, value is another map - no tuple at first level
    }

    @Test
    void testFindTupleNonExistentField()
    {
        // Test: field that doesn't exist
        CqlField normalField = CqlField.builder("normal_col", CqlField.CqlType.text())
                                       .isValueColumn(true)
                                       .build();
        
        List<CqlField> fields = Collections.singletonList(normalField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("non_existent_field");
        assertNull(result);
    }

    @Test
    void testFindTupleNonTupleField()
    {
        // Test: field that exists but is not a tuple
        CqlField normalField = CqlField.builder("normal_col", CqlField.CqlType.text())
                                       .isValueColumn(true)
                                       .build();
        
        List<CqlField> fields = Collections.singletonList(normalField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("normal_col");
        assertNull(result);
    }

    @Test
    void testFindTupleComplexNestedStructure()
    {
        // Test: frozen<map<tuple<int, text>, list<tuple<text, int>>>>
        CqlField.CqlTuple keyTuple = CqlField.CqlTuple.builder()
                                                      .withType(CqlField.CqlType.cint())
                                                      .withType(CqlField.CqlType.text())
                                                      .build();
        
        CqlField.CqlTuple valueTuple = CqlField.CqlTuple.builder()
                                                        .withType(CqlField.CqlType.text())
                                                        .withType(CqlField.CqlType.cint())
                                                        .build();
        
        CqlField.CqlList listOfTuples = CqlField.CqlType.list(valueTuple);
        CqlField.CqlMap mapOfTuples = CqlField.CqlType.map(keyTuple, listOfTuples);
        CqlField.CqlFrozen frozenComplexMap = CqlField.CqlType.frozen(mapOfTuples);
        
        CqlField complexField = CqlField.builder("complex_field", frozenComplexMap)
                                        .isValueColumn(true)
                                        .build();
        
        List<CqlField> fields = Collections.singletonList(complexField);
        CqlTable table = createTestTable(fields);
        
        CqlField.CqlTuple result = table.findTuple("complex_field");
        assertNotNull(result);
        // Should find the key tuple (first one found)
        assertThat(result.size()).isEqualTo(2);
    }

    private CqlTable createTestTable(List<CqlField> fields)
    {
        // Add a primary key field since CqlTable requires it
        CqlField pkField = CqlField.builder("id", CqlField.CqlType.bigint())
                                   .isPartitionKey(true)
                                   .build();
        
        java.util.List<CqlField> allFields = new java.util.ArrayList<>();
        allFields.add(pkField);
        allFields.addAll(fields);
        
        return new CqlTable(
            "test_ks",
            "test_table", 
            "CREATE TABLE test_ks.test_table (...)",
            ReplicationFactor.simple(1),
            allFields
        );
    }
}