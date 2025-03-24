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

package org.apache.cassandra.cdc.avro;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroSchemasTest
{
    @Test
    public void testUnwrapNullableSchema()
    {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON_NORMAL);
        Schema nullable = schema.getField("a").schema();
        assertEquals(Schema.Type.UNION, nullable.getType());
        Schema unwrapped = AvroSchemas.unwrapNullable(nullable);
        assertEquals(Schema.Type.INT, unwrapped.getType());
        // assert the cqlType in schema
        assertEquals("int", AvroSchemas.cqlType(unwrapped));

        // now run unwrap again. It should be the same schema instance
        Schema unwrapped2 = AvroSchemas.unwrapNullable(unwrapped);
        assertSame(unwrapped, unwrapped2);
        // also test the frozen flag
        assertFalse(AvroSchemas.isFrozen(unwrapped));
    }

    @Test
    public void testFrozenFlag()
    {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON_FROZEN);
        Schema nullable = schema.getField("a").schema();
        assertEquals(Schema.Type.UNION, nullable.getType());
        Schema unwrapped = AvroSchemas.unwrapNullable(nullable);
        assertTrue(AvroSchemas.isFrozen(unwrapped));
    }

    @Test
    public void testCqlType()
    {
        for (String cqlType : Arrays.asList("int", "text", "ascii", "bigint", "smallint", "inet"))
        {
            Schema schema = new Schema.Parser().parse(String.format(SCHEMA_JSON_CQLTYPE_FORMATTER, cqlType));
            Schema unwrapped = AvroSchemas.unwrapNullable(schema.getField("a").schema());
            assertEquals(cqlType, AvroSchemas.cqlType(unwrapped));
        }
    }

    @Test
    public void testPrimaryKeys()
    {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON_PRIMARY_KEYS);
        assertEquals(Arrays.asList("a", "b", "c", "d"), AvroSchemas.primaryKeys(schema));
        assertEquals(Collections.singletonList("a"), AvroSchemas.partitionKeys(schema));
        assertEquals(Arrays.asList("b", "c", "d"), AvroSchemas.clusteringKeys(schema));
        assertEquals(Arrays.asList("e", "f", "g"), AvroSchemas.staticColumns(schema));
    }

    private static final String SCHEMA_JSON_NORMAL =
    "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test.test\",\"doc\":\"doc\"," +
    "\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\",\"cqlType\":\"int\"},\"null\"]," +
    "\"doc\":\"doc\"}]}";
    private static final String SCHEMA_JSON_FROZEN =
    "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test.test\",\"doc\":\"doc\"," +
    "\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\",\"isFrozen\":\"true\",\"cqlType\":\"int\"},\"null\"]," +
    "\"doc\":\"doc\"}]}";
    private static final String SCHEMA_JSON_CQLTYPE_FORMATTER =
    "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test.test\",\"doc\":\"doc\"," +
    "\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\",\"cqlType\":\"%s\"},\"null\"]," +
    "\"doc\":\"doc\"}]}";
    private static final String SCHEMA_JSON_PRIMARY_KEYS =
    "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test.test\",\"doc\":\"doc\"," +
    "\"fields\":[{\"name\":\"a\",\"type\":[{\"type\":\"int\"},\"null\"],\"doc\":\"doc\"}]," +
    "\"primary_keys\":[\"a\",\"b\",\"c\",\"d\"],\"partition_keys\":[\"a\"]," +
    "\"clustering_keys\":[\"b\",\"c\",\"d\"],"  +
    "\"static_columns\":[\"e\",\"f\",\"g\"]}";
}
