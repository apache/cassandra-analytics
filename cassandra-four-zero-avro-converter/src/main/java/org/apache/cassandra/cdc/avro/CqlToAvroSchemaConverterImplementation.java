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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.cassandra.bridge.BridgeInitializationParameters;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraSchema;
import org.apache.cassandra.bridge.CassandraTypesImplementation;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;

import static org.apache.cassandra.cdc.avro.AvroConstants.ARRAY_BASED_MAP_KEY_NAME;
import static org.apache.cassandra.cdc.avro.AvroConstants.ARRAY_BASED_MAP_VALUE_NAME;
import static org.apache.cassandra.cdc.avro.AvroConstants.INET_NAME;

/**
 * Cassandra 4.0 implementation of `CqlToAvroSchemaConverter`
 */
public class CqlToAvroSchemaConverterImplementation implements CqlToAvroSchemaConverter
{
    private static final Pattern KEYSPACE_TABLE_MATCH = Pattern.compile("CREATE TABLE (\\S+)\\.(\\S+)");
    public CassandraBridge bridge;

    static
    {
        CassandraTypesImplementation.setup(BridgeInitializationParameters.fromEnvironment());
    }

    public CqlToAvroSchemaConverterImplementation()
    {
        // NOTE: CassandraBridge must be injected separately
    }

    public CqlToAvroSchemaConverterImplementation(CassandraBridge bridge)
    {
        this.bridge = bridge;
    }

    public Schema convert(CassandraBridge bridge, String tableCreateStatement)
    {
        return convert(bridge, tableCreateStatement, Collections.emptySet());
    }

    public Schema convert(CassandraBridge bridge, String tableCreateStatement, Set<String> udts)
    {
        Matcher m = KEYSPACE_TABLE_MATCH.matcher(tableCreateStatement);
        if (!m.find() || m.groupCount() != 2)
        {
            throw new IllegalArgumentException("Invalid create table statement");
        }

        String keyspace = m.group(1);
        String table = m.group(2);
        return convert(keyspace, tableCreateStatement, udts);
    }

    @Override
    public CassandraBridge cassandraBridge()
    {
        return bridge;
    }

    @Override
    public Schema convert(CqlTable cqlTable)
    {
        String keyspace = cqlTable.keyspace();
        String table = cqlTable.table();
        TableMetadata tableMetadata = CassandraSchema.getTable(keyspace, table)
                                                     .orElseThrow(() -> new NoSuchElementException(
                                                     String.format("Table %s/%s is not defined!", keyspace, table)));

        List<Schema.Field> fields = new ArrayList<>(cqlTable.numFields());
        List<AbstractType<?>> columnTypes = new ArrayList<>(cqlTable.numFields());
        for (CqlField cqlField : cqlTable.fields())
        {
            columnTypes.add(tableMetadata.getColumn(new ColumnIdentifier(cqlField.name(), false)).type);
        }
        UdtNames udtNames = new UdtNames(columnTypes);
        List<String> primaryKeys = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();
        List<String> clusteringKeys = new ArrayList<>();
        List<String> staticColumns = new ArrayList<>();
        String topLevelNamespace = String.format("%s.%s", keyspace, table);

        for (CqlField cqlField : cqlTable.fields())
        {
            ColumnMetadata column = tableMetadata.getColumn(new ColumnIdentifier(cqlField.name(), false));
            Schema avroField = schemaFrom(column, topLevelNamespace, udtNames);
            fields.add(new Schema.Field(column.name.toString(), avroField, "doc", null, Schema.Field.Order.ASCENDING));

            if (column.isPrimaryKeyColumn())
            {
                primaryKeys.add(column.name.toString());

                if (column.isPartitionKey())
                {
                    partitionKeys.add(column.name.toString());
                }
                else
                {
                    clusteringKeys.add(column.name.toString());
                }
            }
            else if (column.isStatic())
            {
                staticColumns.add(column.name.toString());
            }
        }
        Schema avroSchema = Schema.createRecord(table, "doc", topLevelNamespace, false, fields);
        AvroSchemas.setPrimaryKeys(avroSchema, primaryKeys);
        AvroSchemas.setPartitionKeys(avroSchema, partitionKeys);
        AvroSchemas.setClusteringKeys(avroSchema, clusteringKeys);
        AvroSchemas.setStaticColumns(avroSchema, staticColumns);
        return avroSchema;
    }

    private static Schema schemaFrom(ColumnMetadata column, String namespace, UdtNames udtNames)
    {
        Schema schema = schemaFrom(column.type, namespace + '.' + column.name.toString(), udtNames);
        return SchemaBuilder.nullable().type(schema);
    }

    private static boolean isFrozen(AbstractType<?> type)
    {
        return (type.isFreezable() && !type.isMultiCell()) || type instanceof FrozenType;
    }

    private static Schema schemaFrom(AbstractType<?> columnCqlType, String namespace, UdtNames udtNames)
    {
        // If a type is reversed we should convert the base type
        AbstractType<?> cqlType = columnCqlType.isReversed() ? ((ReversedType<?>) columnCqlType).baseType : columnCqlType;

        Schema result;
        // If it's a collection. List and set elements use the collection's namespace: a collection has a single element
        // type, and Avro names cannot contain the CQL type (which is kept in the cqlType property instead).
        if (cqlType instanceof ListType)
        {
            result = SchemaBuilder.array()
                                  .items(schemaFrom(((ListType<?>) cqlType).getElementsType(), namespace, udtNames));
        }
        else if (cqlType instanceof SetType)
        {
            result = SchemaBuilder.array()
                                  .items(schemaFrom(((SetType<?>) cqlType).getElementsType(), namespace, udtNames));
            AvroSchemas.flagArrayAsSet(result);
        }
        else if (cqlType instanceof MapType)
        {
            String arrayBasedMapName = AvroConstants.ARRAY_BASED_MAP_NAME;
            String childNamespace = namespace + '.' + arrayBasedMapName;
            MapType<?, ?> cqlMap = (MapType<?, ?>) cqlType;
            // Map entry records nested in both the key and the value are namespaced by key and value so they do not
            // share a full name. Other maps keep their namespace, so their existing schemas do not change.
            boolean bothContainMaps = containsMap(cqlMap.getKeysType()) && containsMap(cqlMap.getValuesType());
            String keyNamespace = bothContainMaps ? childNamespace + '.' + ARRAY_BASED_MAP_KEY_NAME : childNamespace;
            String valueNamespace = bothContainMaps ? childNamespace + '.' + ARRAY_BASED_MAP_VALUE_NAME : childNamespace;
            // create a new avro schema, array of records, to simulate a map.
            SchemaBuilder.FieldAssembler<Schema> keyValue = SchemaBuilder.builder()
                                                                         .record(arrayBasedMapName)
                                                                         .namespace(childNamespace)
                                                                         .fields();
            // map keys and values are not nullable
            keyValue.name(ARRAY_BASED_MAP_KEY_NAME)
                    .type(schemaFrom(cqlMap.getKeysType(), keyNamespace, udtNames))
                    .noDefault();
            keyValue.name(ARRAY_BASED_MAP_VALUE_NAME)
                    .type(schemaFrom(cqlMap.getValuesType(), valueNamespace, udtNames))
                    .noDefault();
            result = SchemaBuilder.builder()
                                  .array()
                                  .items(keyValue.endRecord());
            AvroSchemas.flagArrayAsMap(result);
        }
        else if (cqlType instanceof UserType)
        {
            // A UDT record is named <keyspace>.<udt> wherever the UDT is used, so Avro defines it once in the schema
            // and references it by name elsewhere. Records nested in its fields are namespaced by the UDT, not by its
            // position; the record_udt segment keeps them apart from the records of a table named like the UDT.
            UserType userType = (UserType) cqlType;
            // the Avro name is a valid, unique form of the CQL name, which is kept in the cqlType property
            String udtName = udtNames.avroName(userType);
            String fieldNamespace = userType.keyspace + '.' + udtName + '.' + AvroConstants.RECORD_BASED_UDT_NAME;
            SchemaBuilder.FieldAssembler<Schema> udtValue = SchemaBuilder.builder()
                                                                         .record(udtName)
                                                                         .namespace(userType.keyspace)
                                                                         .fields();
            for (int i = 0; i < userType.size(); i++)
            {
                String fieldName = userType.fieldNameAsString(i);
                udtValue.name(fieldName)
                        .type(schemaFrom(userType.fieldType(i), fieldNamespace + '.' + fieldName, udtNames))
                        .noDefault();
            }
            result = udtValue.endRecord();
            AvroSchemas.flagAsUdt(result);
            // the record is shared by all uses of the UDT, so properties that depend on the use (frozen, reversed) are not set
            AvroSchemas.flagCqlType(result, userType.getNameAsString());
            return result;
        }
        else
        {
            result = convertLiteralType(cqlType, namespace);
        }
        AvroSchemas.flagCqlType(result, cqlType.asCQL3Type().toString());

        if (isFrozen(cqlType))
        {
            AvroSchemas.flagFrozen(result);
        }
        if (columnCqlType.isReversed())
        {
            AvroSchemas.flagReversed(result);
        }

        return result;
    }

    /**
     * Avro names must match [A-Za-z_][A-Za-z0-9_]*, but quoted CQL identifiers can contain any character.
     *
     * @return the name with every illegal character replaced by an underscore
     */
    static String sanitizeAvroName(String name)
    {
        StringBuilder sanitized = new StringBuilder(name.length() + 1);
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            boolean valid = c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9');
            if (!valid && i == 0 && c >= '0' && c <= '9')
            {
                sanitized.append('_');
                valid = true;
            }
            sanitized.append(valid ? c : '_');
        }
        return sanitized.length() == 0 ? "_" : sanitized.toString();
    }

    /**
     * Assigns each UDT of a table a distinct Avro record name. A name that is already valid is never changed. A
     * sanitized name that collides with another gets a number appended, starting at 2.
     */
    private static final class UdtNames
    {
        private final Map<List<String>, String> names = new HashMap<>();
        private final Set<String> taken = new HashSet<>();

        UdtNames(Collection<AbstractType<?>> columnTypes)
        {
            Map<List<String>, UserType> udts = new LinkedHashMap<>();
            columnTypes.forEach(type -> collect(type, udts));
            // names that are valid as they are claim them first
            udts.values().stream().filter(udt -> sanitizeAvroName(udt.getNameAsString()).equals(udt.getNameAsString()))
                .forEach(this::avroName);
            udts.values().forEach(this::avroName);
        }

        String avroName(UserType udt)
        {
            return names.computeIfAbsent(List.of(udt.keyspace, udt.getNameAsString()), key -> {
                String base = sanitizeAvroName(udt.getNameAsString());
                String candidate = base;
                for (int suffix = 2; !taken.add(udt.keyspace + '.' + candidate); suffix++)
                {
                    candidate = base + suffix;
                }
                return candidate;
            });
        }

        private static void collect(AbstractType<?> type, Map<List<String>, UserType> udts)
        {
            if (type.isReversed())
            {
                collect(((ReversedType<?>) type).baseType, udts);
            }
            else if (type instanceof ListType)
            {
                collect(((ListType<?>) type).getElementsType(), udts);
            }
            else if (type instanceof SetType)
            {
                collect(((SetType<?>) type).getElementsType(), udts);
            }
            else if (type instanceof MapType)
            {
                collect(((MapType<?, ?>) type).getKeysType(), udts);
                collect(((MapType<?, ?>) type).getValuesType(), udts);
            }
            else if (type instanceof UserType)
            {
                UserType udt = (UserType) type;
                if (udts.putIfAbsent(List.of(udt.keyspace, udt.getNameAsString()), udt) == null)
                {
                    udt.fieldTypes().forEach(fieldType -> collect(fieldType, udts));
                }
            }
        }
    }

    /**
     * @return whether the type is a map, or a list or set of maps (maps nested in UDTs are namespaced by the UDT)
     */
    private static boolean containsMap(AbstractType<?> cqlType)
    {
        if (cqlType instanceof MapType)
        {
            return true;
        }
        if (cqlType instanceof ListType)
        {
            return containsMap(((ListType<?>) cqlType).getElementsType());
        }
        if (cqlType instanceof SetType)
        {
            return containsMap(((SetType<?>) cqlType).getElementsType());
        }
        return false;
    }

    /**
     * Following link contains details about all cql types in cassandra.
     * https://cassandra.apache.org/doc/latest/cassandra/cql/types.html
     *
     * @param cqlType   cql column type
     * @param namespace namespace
     * @return schema of the field
     */
    private static Schema convertLiteralType(AbstractType<?> cqlType, String namespace)
    {
        Schema result;
        if (cqlType instanceof AsciiType)
        {
            //ascii
            result = SchemaBuilder.builder(namespace).stringType();
        }
        else if (cqlType instanceof LongType)
        {
            //big int
            result = SchemaBuilder.builder(namespace).longType();
        }
        else if (cqlType instanceof BytesType)
        {
            //blob
            result = SchemaBuilder.builder(namespace).bytesType();
        }
        else if (cqlType instanceof BooleanType)
        {
            // boolean
            result = SchemaBuilder.builder(namespace).booleanType();
        }
        else if (cqlType instanceof SimpleDateType)
        {
            // Date with logical type date
            result = SchemaBuilder.builder(namespace).intType();
            LogicalTypes.date().addToSchema(result);
        }
        else if (cqlType instanceof DecimalType)
        {
            // fixed with logical type decimal
            result = SchemaBuilder.builder(namespace).fixed(".fixed").size(16);
            LogicalTypes.decimal(38, 19).addToSchema(result);
        }
        else if (cqlType instanceof DoubleType)
        {
            // double
            result = SchemaBuilder.builder(namespace).doubleType();
        }
        else if (cqlType instanceof FloatType)
        {
            // Float
            result = SchemaBuilder.builder(namespace).floatType();
        }
        else if (cqlType instanceof InetAddressType)
        {
            // Inet address
            result = SchemaBuilder.builder(namespace).bytesType();
            new LogicalType(INET_NAME).addToSchema(result);
        }
        else if (cqlType instanceof Int32Type || cqlType instanceof ShortType)
        {
            // int 32 & smallint
            result = SchemaBuilder.builder(namespace).intType();
        }
        else if (cqlType instanceof UTF8Type)
        {
            // text & varchar
            result = SchemaBuilder.builder(namespace).stringType();
        }
        else if (cqlType instanceof TimeType)
        {
            // time
            result = SchemaBuilder.builder(namespace).longType();
        }
        else if (cqlType instanceof TimestampType)
        {
            // timestamp
            result = SchemaBuilder.builder(namespace).longType();
            LogicalTypes.timestampMicros().addToSchema(result);
        }
        else if (cqlType instanceof TimeUUIDType || cqlType instanceof UUIDType)
        {
            // timeuuid, uuid
            result = SchemaBuilder.builder(namespace).stringType();
            LogicalTypes.uuid().addToSchema(result);
        }
        else if (cqlType instanceof ByteType)
        {
            // tinyint
            result = SchemaBuilder.builder(namespace).intType();
        }
        else if (cqlType instanceof IntegerType)
        {
            // varint
            result = SchemaBuilder.builder(namespace).fixed(".fixed").size(16);
            LogicalTypes.decimal(38, 0).addToSchema(result);
        }
        else if (cqlType instanceof DurationType || cqlType instanceof EmptyType || cqlType instanceof CounterColumnType)
        {
            throw new UnsupportedOperationException("Unsupported Cql data type " + cqlType.asCQL3Type());
            // not supported
        }
        else
        {
            throw new RuntimeException("Unknown Cql datatype " + cqlType.asCQL3Type());
        }
        return result;
    }
}
