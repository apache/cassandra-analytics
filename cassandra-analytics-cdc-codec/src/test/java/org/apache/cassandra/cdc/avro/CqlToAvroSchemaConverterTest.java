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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.cdc.schemastore.LocalTableSchemaStore;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;
import static org.apache.cassandra.spark.utils.ArrayUtils.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CqlToAvroSchemaConverterTest
{
    private static volatile int id = 0;
    private static final String expected_avro_string = "{\n" +
                                                       "  \"type\" : \"record\",\n" +
                                                       "  \"name\" : \"tb\",\n" +
                                                       "  \"namespace\" : \"ks.tb\",\n" +
                                                       "  \"doc\" : \"doc\",\n" +
                                                       "  \"fields\" : [ {\n" +
                                                       "    \"name\" : \"a\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"string\",\n" +
                                                       "      \"cqlType\" : \"text\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"b\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"string\",\n" +
                                                       "      \"cqlType\" : \"text\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"c\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"bytes\",\n" +
                                                       "      \"cqlType\" : \"blob\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"d\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"bytes\",\n" +
                                                       "      \"cqlType\" : \"blob\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"e\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"bytes\",\n" +
                                                       "      \"cqlType\" : \"blob\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"f\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"long\",\n" +
                                                       "      \"logicalType\" : \"timestamp-micros\",\n" +
                                                       "      \"cqlType\" : \"timestamp\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"g\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"long\",\n" +
                                                       "      \"cqlType\" : \"bigint\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"h\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"long\",\n" +
                                                       "      \"logicalType\" : \"timestamp-micros\",\n" +
                                                       "      \"cqlType\" : \"timestamp\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"i\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"long\",\n" +
                                                       "      \"logicalType\" : \"timestamp-micros\",\n" +
                                                       "      \"cqlType\" : \"timestamp\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"j\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"string\",\n" +
                                                       "      \"cqlType\" : \"text\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"k\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"long\",\n" +
                                                       "      \"logicalType\" : \"timestamp-micros\",\n" +
                                                       "      \"cqlType\" : \"timestamp\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"l\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"string\",\n" +
                                                       "      \"cqlType\" : \"text\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"m\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"array\",\n" +
                                                       "      \"items\" : {\n" +
                                                       "        \"type\" : \"record\",\n" +
                                                       "        \"name\" : \"array_map\",\n" +
                                                       "        \"namespace\" : \"ks.tb.m.array_map\",\n" +
                                                       "        \"fields\" : [ {\n" +
                                                       "          \"name\" : \"key\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        }, {\n" +
                                                       "          \"name\" : \"value\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        } ]\n" +
                                                       "      },\n" +
                                                       "      \"logicalType\" : \"array_map\",\n" +
                                                       "      \"cqlType\" : \"map<text, text>\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"n\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"array\",\n" +
                                                       "      \"items\" : {\n" +
                                                       "        \"type\" : \"record\",\n" +
                                                       "        \"name\" : \"array_map\",\n" +
                                                       "        \"namespace\" : \"ks.tb.n.array_map\",\n" +
                                                       "        \"fields\" : [ {\n" +
                                                       "          \"name\" : \"key\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        }, {\n" +
                                                       "          \"name\" : \"value\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        } ]\n" +
                                                       "      },\n" +
                                                       "      \"logicalType\" : \"array_map\",\n" +
                                                       "      \"cqlType\" : \"map<text, text>\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"o\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"array\",\n" +
                                                       "      \"items\" : {\n" +
                                                       "        \"type\" : \"record\",\n" +
                                                       "        \"name\" : \"array_map\",\n" +
                                                       "        \"namespace\" : \"ks.tb.o.array_map\",\n" +
                                                       "        \"fields\" : [ {\n" +
                                                       "          \"name\" : \"key\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        }, {\n" +
                                                       "          \"name\" : \"value\",\n" +
                                                       "          \"type\" : {\n" +
                                                       "            \"type\" : \"string\",\n" +
                                                       "            \"cqlType\" : \"text\"\n" +
                                                       "          }\n" +
                                                       "        } ]\n" +
                                                       "      },\n" +
                                                       "      \"logicalType\" : \"array_map\",\n" +
                                                       "      \"cqlType\" : \"map<text, text>\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"p\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"array\",\n" +
                                                       "      \"items\" : {\n" +
                                                       "        \"type\" : \"string\",\n" +
                                                       "        \"cqlType\" : \"text\"\n" +
                                                       "      },\n" +
                                                       "      \"logicalType\" : \"array_set\",\n" +
                                                       "      \"cqlType\" : \"set<text>\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  }, {\n" +
                                                       "    \"name\" : \"q\",\n" +
                                                       "    \"type\" : [ {\n" +
                                                       "      \"type\" : \"array\",\n" +
                                                       "      \"items\" : {\n" +
                                                       "        \"type\" : \"string\",\n" +
                                                       "        \"cqlType\" : \"text\"\n" +
                                                       "      },\n" +
                                                       "      \"logicalType\" : \"array_set\",\n" +
                                                       "      \"cqlType\" : \"set<text>\"\n" +
                                                       "    }, \"null\" ],\n" +
                                                       "    \"doc\" : \"doc\"\n" +
                                                       "  } ],\n" +
                                                       "  \"primary_keys\" : [ \"a\", \"b\" ],\n" +
                                                       "  \"partition_keys\" : [ \"a\" ],\n" +
                                                       "  \"clustering_keys\" : [ \"b\" ],\n" +
                                                       "  \"static_columns\" : [ ]\n" +
                                                       '}';

    CqlToAvroSchemaConverter cqlToAvroSchemaConverter = new CqlToAvroSchemaConverterImplementation(new CassandraBridgeImplementation());

    @Test
    public void testCqlToAvroSchemaString()
    {
        // example how to convert Cassandra CQL table schema into Avro
        final String tableCreateStmt = "CREATE TABLE ks.tb (\n" +
                                       "    a text,\n" +
                                       "    b text,\n" +
                                       "    c blob,\n" +
                                       "    d blob,\n" +
                                       "    e blob,\n" +
                                       "    f timestamp,\n" +
                                       "    g bigint,\n" +
                                       "    h timestamp,\n" +
                                       "    i timestamp,\n" +
                                       "    j text,\n" +
                                       "    k timestamp,\n" +
                                       "    l text,\n" +
                                       "    m map<text, text>,\n" +
                                       "    n map<text, text>,\n" +
                                       "    o map<text, text>,\n" +
                                       "    p set<text>,\n" +
                                       "    q set<text>,\n" +
                                       "    PRIMARY KEY (a, b)\n" +
                                       ") WITH CLUSTERING ORDER BY (b ASC);";
        String avroStringjsonString = cqlToAvroSchemaConverter.schemaStringFromCql("ks", tableCreateStmt);
        assertEquals(expected_avro_string, avroStringjsonString);
    }

    @Test
    public void testStringMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<text, blob>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertNotNull(schema.toString(true));
        Schema nullableMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(nullableMap,
                                    key -> assertEquals(STRING, key.getType()),
                                    value -> assertEquals(BYTES, value.getType()));
        assertEquals("int", readCqlType(schema, "an"));
        assertEquals("map<text, blob>", readCqlType(schema, "m"));
    }

    @Test
    public void testMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<int, blob>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertNotNull(schema.toString(true));
        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    key -> assertEquals(INT, key.getType()),
                                    value -> assertEquals(BYTES, value.getType()));
        assertEquals("int", readCqlType(schema, "an"));
        assertEquals("map<int, blob>", readCqlType(schema, "m"));
    }

    @Test
    public void testUdtType()
    {
        final String udt1 = "CREATE TYPE udt_1 (ids frozen<list<text>>, a int, b bigint, c text);";
        final String udt2 = "CREATE TYPE udt_2 (id text, show boolean);";
        String createStmt = decorateTable("CREATE TABLE %s (pk int PRIMARY KEY, c1 udt_1, c2 frozen<list<frozen<udt_2>>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt, setOf(udt1, udt2));
        assertNotNull(schema.toString(true));

        Schema c1 = schema.getField("c1").schema();
        assertNullable(c1, udt -> {
            assertEquals(RECORD, udt.getType());
            assertTrue(AvroSchemas.isRecordBasedUdt(udt));

            Schema ids = udt.getField("ids").schema();
            assertNotNull(ids);
            assertTrue(AvroSchemas.isFrozen(ids));
            assertEquals(ARRAY, ids.getType());
            assertEquals(STRING, ids.getElementType().getType());

            Schema a = udt.getField("a").schema();
            assertNotNull(a);
            assertFalse(AvroSchemas.isFrozen(a));
            assertEquals(INT, a.getType());

            Schema b = udt.getField("b").schema();
            assertNotNull(b);
            assertFalse(AvroSchemas.isFrozen(b));
            assertEquals(LONG, b.getType());

            Schema c = udt.getField("c").schema();
            assertNotNull(c);
            assertFalse(AvroSchemas.isFrozen(c));
            assertEquals(STRING, c.getType());
        });

        Schema c2 = schema.getField("c2").schema();
        assertNullable(c2, type -> {
            assertEquals(ARRAY, type.getType());
            assertTrue(AvroSchemas.isFrozen(type));
            assertEquals("frozen<list<frozen<udt_2>>>", AvroSchemas.cqlType(AvroSchemas.unwrapNullable(type)));

            final Schema innerUdt = type.getElementType();
            assertEquals(RECORD, innerUdt.getType());
            assertTrue(AvroSchemas.isFrozen(innerUdt));
            assertEquals("frozen<udt_2>", AvroSchemas.cqlType(AvroSchemas.unwrapNullable(innerUdt)));

            Schema id = innerUdt.getField("id").schema();
            assertNotNull(id);
            assertEquals(STRING, id.getType());

            Schema show = innerUdt.getField("show").schema();
            assertNotNull(show);
            assertEquals(BOOLEAN, show.getType());
        });

        assertEquals("int", readCqlType(schema, "pk"));
        assertEquals("udt_1", readCqlType(schema, "c1"));
        assertEquals("frozen<list<frozen<udt_2>>>", readCqlType(schema, "c2"));
    }

    @Test
    public void testNestedMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<frozen<set<int>>, frozen<map<int, int>>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertNotNull(schema.toString(true));

        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    key -> {
                                        assertEquals(ARRAY, key.getType());
                                        assertEquals(INT, key.getElementType().getType());
                                        assertTrue(AvroSchemas.isFrozen(key));
                                        assertTrue(AvroSchemas.isArrayBasedSet(key));
                                    },
                                    value -> {
                                        assertTrue(AvroSchemas.isFrozen(value), value.toString());
                                        assertArrayBasedMap(value,
                                                            innerKey -> assertEquals(INT, innerKey.getType()),
                                                            innerValue -> assertEquals(INT, innerValue.getType()));
                                    });
        assertEquals("int", readCqlType(schema, "an"));
        assertEquals("map<frozen<set<int>>, frozen<map<int, int>>>", readCqlType(schema, "m"));
    }

    @Test
    public void testFrozenType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m frozen<map<int, blob>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertNotNull(schema.toString(true));
        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    arrayMap -> assertTrue(AvroSchemas.isFrozen(arrayMap)),
                                    key -> assertEquals(INT, key.getType()),
                                    value -> assertEquals(BYTES, value.getType()));
        assertTrue(AvroSchemas.isFrozen(AvroSchemas.unwrapNullable(schema.getField("m").schema())));
        assertEquals("int", readCqlType(schema, "an"));
        assertEquals("frozen<map<int, blob>>", readCqlType(schema, "m"));
    }

    @Test
    public void testSetType()
    {
        String createStatement = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, s set<int>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStatement);
        assertNotNull(schema.toString());
        Schema nullableSet = schema.getField("s").schema();
        assertNullable(nullableSet, set -> {
            assertSame(ARRAY, set.getType());
            assertEquals(INT, set.getElementType().getType());
            assertTrue(AvroSchemas.isArrayBasedSet(set));
        });
        assertEquals("int", readCqlType(schema, "an"));
        assertEquals("set<int>", readCqlType(schema, "s"));
    }

    @Test
    public void testUuidType()
    {
        testLogicalType("uuid", STRING, LogicalTypes.uuid());
    }

    @Test
    public void testTimeUuidType()
    {
        testLogicalType("timeuuid", STRING, LogicalTypes.uuid());
    }

    @Test
    public void testDateType()
    {
        testLogicalType("date", INT, LogicalTypes.date());
    }

    @Test
    public void testInetType()
    {
        testLogicalType("inet", BYTES, new LogicalType(AvroSchemas.INET_NAME));
    }

    @Test
    public void testVarIntType()
    {
        testLogicalType("varint", FIXED, LogicalTypes.decimal(38, 0));
    }

    @Test
    public void testDecimalType()
    {
        testLogicalType("decimal", FIXED, LogicalTypes.decimal(38, 19));
    }

    @Test
    public void testTimeStampMillis()
    {
        testLogicalType("timestamp", LONG, LogicalTypes.timestampMicros());
    }


    @Test
    public void testSchemaChange()
    {
        String cqlSchema1 = "CREATE TABLE test_ks.test_tbl (a int PRIMARY KEY, b int);";
        Schema schema1 = cqlToAvroSchemaConverter.convert("test_ks", cqlSchema1);
        String cqlSchema2 = "CREATE TABLE test_ks.test_tbl (a int PRIMARY KEY, b int, c int);";
        Schema schema2 = cqlToAvroSchemaConverter.convert("test_ks", cqlSchema2);
        assertNotEquals(schema1, schema2);
    }

    @Test
    public void testSchemaGenerationForSchemasInResources() throws IOException
    {
        URL folder = Resources.getResource("cql_schemas");
        Files.walk(Paths.get(folder.getPath())).filter(Files::isRegularFile).forEach(path -> {
            try
            {
                String cqlSchema = Resources.toString(path.toUri().toURL(), StandardCharsets.UTF_8);
                Schema generatedSchema = cqlToAvroSchemaConverter.convert("test_ks", cqlSchema);
                String[] keyspaceTable = generatedSchema.getNamespace().split("\\.");
                Schema expectedSchema = LocalTableSchemaStore.getInstance().getSchema(keyspaceTable[0] + '.' + keyspaceTable[1], null);
                assertEquals(expectedSchema, generatedSchema);
            }
            catch (IOException e)
            {
                fail(e);
            }
        });
    }

    @Test
    public void testReversed()
    {
        String cqlSchema = "CREATE TABLE test_ks.test_tbl3 (\n" +
                           "    a uuid,\n" +
                           "    b timeuuid,\n" +
                           "    c timestamp static,\n" +
                           "    PRIMARY KEY (a, b)\n" +
                           ") WITH CLUSTERING ORDER BY (b DESC)";
        Schema schema = cqlToAvroSchemaConverter.convert("test_ks", cqlSchema);
        assertEquals("true", schema.getField("b").schema().getTypes().get(0).getProp("isReversed"));
    }

    private void testLogicalType(String cqlType, Schema.Type expectedAvroType, LogicalType expectedLogicalType)
    {
        String createStmt = decorateTable("CREATE TABLE %s (id " + cqlType + " PRIMARY KEY, val text);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertNotNull(schema.toString(true));
        Schema testFieldSchema = schema.getField("id").schema();
        assertNullable(testFieldSchema, field -> {
            assertEquals(expectedAvroType, field.getType());
            assertEquals(expectedLogicalType.getName(), field.getLogicalType().getName());
            assertEquals(cqlType, AvroSchemas.cqlType(field));
        });
    }

    private String decorateTable(String format)
    {
        return String.format(format, "a.tbl" + id++);
    }

    private static void assertNullableArrayBasedMap(Schema nullable,
                                                    Consumer<Schema> keyVerifier,
                                                    Consumer<Schema> valueVerifier)
    {
        assertNullableArrayBasedMap(nullable, x -> {
        }, keyVerifier, valueVerifier);
    }

    private static void assertNullableArrayBasedMap(Schema nullable,
                                                    Consumer<Schema> arrayBasedMapVerifier,
                                                    Consumer<Schema> keyVerifier,
                                                    Consumer<Schema> valueVerifier)
    {
        assertNullable(nullable, s -> {
            arrayBasedMapVerifier.accept(s);
            assertArrayBasedMap(s, keyVerifier, valueVerifier);
        });
    }

    private static void assertNullable(Schema nullable, Consumer<Schema> actualTypeVerifier)
    {
        assertEquals(UNION, nullable.getType());
        assertEquals(2, nullable.getTypes().size());
        boolean hasNull = false;
        for (Schema s : nullable.getTypes())
        {
            if (s.getType() == NULL)
            {
                hasNull = true;
            }
            else
            {
                actualTypeVerifier.accept(s);
            }
        }
        assertTrue(hasNull);
    }

    private static void assertArrayBasedMap(Schema arrayBasedMap, Consumer<Schema> keyVerifier, Consumer<Schema> valueVerifier)
    {
        assertEquals(ARRAY, arrayBasedMap.getType());
        assertTrue(AvroSchemas.isArrayBasedMap(arrayBasedMap));
        Schema keyValue = arrayBasedMap.getElementType();
        assertEquals(RECORD, keyValue.getType());
        keyVerifier.accept(keyValue.getField(AvroSchemas.ARRAY_BASED_MAP_KEY_NAME).schema());
        valueVerifier.accept(keyValue.getField(AvroSchemas.ARRAY_BASED_MAP_VALUE_NAME).schema());
    }

    private static String readCqlType(Schema schema, String field)
    {
        return AvroSchemas.cqlType(AvroSchemas.unwrapNullable(schema.getField(field).schema()));
    }
}
