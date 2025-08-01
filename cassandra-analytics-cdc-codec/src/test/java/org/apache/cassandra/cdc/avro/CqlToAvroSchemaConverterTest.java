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
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CqlToAvroSchemaConverterTest
{
    private static volatile int id = 0;

    CqlToAvroSchemaConverter cqlToAvroSchemaConverter = new CqlToAvroSchemaConverterImplementation(new CassandraBridgeImplementation());

    @Test
    public void testCqlToAvroSchemaString() throws IOException
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
        String expectedAvroString = null;
        try (InputStream inputStream = Objects
                                       .requireNonNull(CqlToAvroSchemaConverter.class.getResource("/expected.avro"), "Could not find expected.avro resource")
                                       .openStream())
        {
            expectedAvroString = new String(IOUtils.toByteArray(inputStream), StandardCharsets.UTF_8);
        }
        assertThat(avroStringjsonString).as("Generated Avro schema should match expected schema").isEqualTo(expectedAvroString);
    }

    @Test
    public void testStringMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<text, blob>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();
        Schema nullableMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(nullableMap,
                                    key -> assertThat(key.getType()).as("Map key type should be STRING").isEqualTo(STRING),
                                    value -> assertThat(value.getType()).as("Map value type should be BYTES").isEqualTo(BYTES));
        assertThat(readCqlType(schema, "an")).as("CQL type for 'an' field").isEqualTo("int");
        assertThat(readCqlType(schema, "m")).as("CQL type for 'm' field").isEqualTo("map<text, blob>");
    }

    @Test
    public void testMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<int, blob>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();
        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    key -> assertThat(key.getType()).as("Map key type should be INT").isEqualTo(INT),
                                    value -> assertThat(value.getType()).as("Map value type should be BYTES").isEqualTo(BYTES));
        assertThat(readCqlType(schema, "an")).as("CQL type for 'an' field").isEqualTo("int");
        assertThat(readCqlType(schema, "m")).as("CQL type for 'm' field").isEqualTo("map<int, blob>");
    }

    @Test
    public void testUdtType()
    {
        final String udt1 = "CREATE TYPE udt_1 (ids frozen<list<text>>, a int, b bigint, c text);";
        final String udt2 = "CREATE TYPE udt_2 (id text, show boolean);";
        String createStmt = decorateTable("CREATE TABLE %s (pk int PRIMARY KEY, c1 udt_1, c2 frozen<list<frozen<udt_2>>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt, setOf(udt1, udt2));
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();

        Schema c1 = schema.getField("c1").schema();
        assertNullable(c1, udt -> {
            assertThat(udt.getType()).as("UDT type should be RECORD").isEqualTo(RECORD);
            assertThat(AvroSchemas.isRecordBasedUdt(udt)).as("Should be record-based UDT").isTrue();

            Schema ids = udt.getField("ids").schema();
            assertThat(ids).as("UDT ids field should not be null").isNotNull();
            assertThat(AvroSchemas.isFrozen(ids)).as("UDT ids field should be frozen").isTrue();
            assertThat(ids.getType()).as("UDT ids field type should be ARRAY").isEqualTo(ARRAY);
            assertThat(ids.getElementType().getType()).as("UDT ids element type should be STRING").isEqualTo(STRING);

            Schema a = udt.getField("a").schema();
            assertThat(a).as("UDT a field should not be null").isNotNull();
            assertThat(AvroSchemas.isFrozen(a)).as("UDT a field should not be frozen").isFalse();
            assertThat(a.getType()).as("UDT a field type should be INT").isEqualTo(INT);

            Schema b = udt.getField("b").schema();
            assertThat(b).as("UDT b field should not be null").isNotNull();
            assertThat(AvroSchemas.isFrozen(b)).as("UDT b field should not be frozen").isFalse();
            assertThat(b.getType()).as("UDT b field type should be LONG").isEqualTo(LONG);

            Schema c = udt.getField("c").schema();
            assertThat(c).as("UDT c field should not be null").isNotNull();
            assertThat(AvroSchemas.isFrozen(c)).as("UDT c field should not be frozen").isFalse();
            assertThat(c.getType()).as("UDT c field type should be STRING").isEqualTo(STRING);
        });

        Schema c2 = schema.getField("c2").schema();
        assertNullable(c2, type -> {
            assertThat(type.getType()).as("c2 type should be ARRAY").isEqualTo(ARRAY);
            assertThat(AvroSchemas.isFrozen(type)).as("c2 should be frozen").isTrue();
            assertThat(AvroSchemas.cqlType(AvroSchemas.unwrapNullable(type))).as("c2 CQL type").isEqualTo("frozen<list<frozen<udt_2>>>");

            final Schema innerUdt = type.getElementType();
            assertThat(innerUdt.getType()).as("Inner UDT type should be RECORD").isEqualTo(RECORD);
            assertThat(AvroSchemas.isFrozen(innerUdt)).as("Inner UDT should be frozen").isTrue();
            assertThat(AvroSchemas.cqlType(AvroSchemas.unwrapNullable(innerUdt))).as("Inner UDT CQL type").isEqualTo("frozen<udt_2>");

            Schema id = innerUdt.getField("id").schema();
            assertThat(id).as("Inner UDT id field should not be null").isNotNull();
            assertThat(id.getType()).as("Inner UDT id field type should be STRING").isEqualTo(STRING);

            Schema show = innerUdt.getField("show").schema();
            assertThat(show).as("Inner UDT show field should not be null").isNotNull();
            assertThat(show.getType()).as("Inner UDT show field type should be BOOLEAN").isEqualTo(BOOLEAN);
        });

        assertThat(readCqlType(schema, "pk")).as("CQL type for 'pk' field").isEqualTo("int");
        assertThat(readCqlType(schema, "c1")).as("CQL type for 'c1' field").isEqualTo("udt_1");
        assertThat(readCqlType(schema, "c2")).as("CQL type for 'c2' field").isEqualTo("frozen<list<frozen<udt_2>>>");
    }

    @Test
    public void testNestedMapType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m map<frozen<set<int>>, frozen<map<int, int>>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();

        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    key -> {
                                        assertThat(key.getType()).as("Map key type should be ARRAY").isEqualTo(ARRAY);
                                        assertThat(key.getElementType().getType()).as("Map key element type should be INT").isEqualTo(INT);
                                        assertThat(AvroSchemas.isFrozen(key)).as("Map key should be frozen").isTrue();
                                        assertThat(AvroSchemas.isArrayBasedSet(key)).as("Map key should be array-based set").isTrue();
                                    },
                                    value -> {
                                        assertThat(AvroSchemas.isFrozen(value)).as("Map value should be frozen: " + value.toString()).isTrue();
                                        assertArrayBasedMap(value,
                                                            innerKey -> assertThat(innerKey.getType()).as("Inner map key type should be INT").isEqualTo(INT),
                                                            innerValue -> assertThat(innerValue.getType()).as("Inner map value type should be INT").isEqualTo(INT));
                                    });
        assertThat(readCqlType(schema, "an")).as("CQL type for 'an' field").isEqualTo("int");
        assertThat(readCqlType(schema, "m")).as("CQL type for 'm' field").isEqualTo("map<frozen<set<int>>, frozen<map<int, int>>>");
    }

    @Test
    public void testFrozenType()
    {
        String createStmt = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, m frozen<map<int, blob>>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();
        Schema arrayBasedMap = schema.getField("m").schema();
        assertNullableArrayBasedMap(arrayBasedMap,
                                    arrayMap -> assertThat(AvroSchemas.isFrozen(arrayMap)).as("Array map should be frozen").isTrue(),
                                    key -> assertThat(key.getType()).as("Map key type should be INT").isEqualTo(INT),
                                    value -> assertThat(value.getType()).as("Map value type should be BYTES").isEqualTo(BYTES));
        assertThat(AvroSchemas.isFrozen(AvroSchemas.unwrapNullable(schema.getField("m").schema()))).as("Unwrapped schema should be frozen").isTrue();
        assertThat(readCqlType(schema, "an")).as("CQL type for 'an' field").isEqualTo("int");
        assertThat(readCqlType(schema, "m")).as("CQL type for 'm' field").isEqualTo("frozen<map<int, blob>>");
    }

    @Test
    public void testSetType()
    {
        String createStatement = decorateTable("CREATE TABLE %s (an int PRIMARY KEY, s set<int>);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStatement);
        assertThat(schema.toString()).as("Schema string should not be null").isNotNull();
        Schema nullableSet = schema.getField("s").schema();
        assertNullable(nullableSet, set -> {
            assertThat(set.getType()).as("Set type should be ARRAY").isSameAs(ARRAY);
            assertThat(set.getElementType().getType()).as("Set element type should be INT").isEqualTo(INT);
            assertThat(AvroSchemas.isArrayBasedSet(set)).as("Should be array-based set").isTrue();
        });
        assertThat(readCqlType(schema, "an")).as("CQL type for 'an' field").isEqualTo("int");
        assertThat(readCqlType(schema, "s")).as("CQL type for 's' field").isEqualTo("set<int>");
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
        testLogicalType("inet", BYTES, new LogicalType(AvroConstants.INET_NAME));
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
        assertThat(schema1).as("Schema1 should not equal schema2 after schema change").isNotEqualTo(schema2);
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
                assertThat(generatedSchema).as("Generated schema should match expected schema for " + path.getFileName()).isEqualTo(expectedSchema);
            }
            catch (IOException e)
            {
                fail("Schema generation fails", e);
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
        assertThat(schema.getField("b").schema().getTypes().get(0).getProp("isReversed"))
        .as("Field 'b' should be marked as reversed")
        .isEqualTo("true");
    }

    private void testLogicalType(String cqlType, Schema.Type expectedAvroType, LogicalType expectedLogicalType)
    {
        String createStmt = decorateTable("CREATE TABLE %s (id " + cqlType + " PRIMARY KEY, val text);");
        Schema schema = cqlToAvroSchemaConverter.convert("a", createStmt);
        assertThat(schema.toString(true)).as("Schema string should not be null").isNotNull();
        Schema testFieldSchema = schema.getField("id").schema();
        assertNullable(testFieldSchema, field -> {
            assertThat(field.getType()).as("Field type should match expected Avro type").isEqualTo(expectedAvroType);
            assertThat(field.getLogicalType().getName()).as("Field logical type name should match expected").isEqualTo(expectedLogicalType.getName());
            assertThat(AvroSchemas.cqlType(field)).as("Field CQL type should match expected").isEqualTo(cqlType);
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
        assertThat(nullable.getType()).as("Nullable schema should be UNION type").isEqualTo(UNION);
        assertThat(nullable.getTypes().size()).as("Nullable schema should have exactly 2 types").isEqualTo(2);
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
        assertThat(hasNull).as("Nullable schema should contain NULL type").isTrue();
    }

    private static void assertArrayBasedMap(Schema arrayBasedMap, Consumer<Schema> keyVerifier, Consumer<Schema> valueVerifier)
    {
        assertThat(arrayBasedMap.getType()).as("Array-based map should be ARRAY type").isEqualTo(ARRAY);
        assertThat(AvroSchemas.isArrayBasedMap(arrayBasedMap)).as("Should be array-based map").isTrue();
        Schema keyValue = arrayBasedMap.getElementType();
        assertThat(keyValue.getType()).as("Key-value record type should be RECORD").isEqualTo(RECORD);
        keyVerifier.accept(keyValue.getField(AvroConstants.ARRAY_BASED_MAP_KEY_NAME).schema());
        valueVerifier.accept(keyValue.getField(AvroConstants.ARRAY_BASED_MAP_VALUE_NAME).schema());
    }

    private static String readCqlType(Schema schema, String field)
    {
        return AvroSchemas.cqlType(AvroSchemas.unwrapNullable(schema.getField(field).schema()));
    }
}
