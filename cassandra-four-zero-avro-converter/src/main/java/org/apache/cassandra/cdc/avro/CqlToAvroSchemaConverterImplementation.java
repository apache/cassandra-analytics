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
import java.util.Collections;
import java.util.List;
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
        List<String> primaryKeys = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();
        List<String> clusteringKeys = new ArrayList<>();
        List<String> staticColumns = new ArrayList<>();
        String topLevelNamespace = String.format("%s.%s", keyspace, table);

        for (CqlField cqlField : cqlTable.fields())
        {
            ColumnMetadata column = tableMetadata.getColumn(new ColumnIdentifier(cqlField.name(), false));
            Schema avroField = schemaFrom(column, topLevelNamespace);
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

    private static Schema schemaFrom(ColumnMetadata column, String namespace)
    {
        Schema schema = schemaFrom(column.type, namespace + '.' + column.name.toString());
        return SchemaBuilder.nullable().type(schema);
    }

    private static boolean isFrozen(AbstractType<?> type)
    {
        return (type.isFreezable() && !type.isMultiCell()) || type instanceof FrozenType;
    }

    private static Schema schemaFrom(AbstractType<?> columnCqlType, String namespace)
    {
        // If a type is reversed we should convert the base type
        AbstractType<?> cqlType = columnCqlType.isReversed() ? ((ReversedType<?>) columnCqlType).baseType : columnCqlType;

        Schema result;
        // If it's a collection
        if (cqlType instanceof ListType)
        {
            String collectionNamespace = namespace + '.' + cqlType.asCQL3Type().toString();
            result = SchemaBuilder.array()
                                  .items(schemaFrom(((ListType<?>) cqlType).getElementsType(), collectionNamespace));
        }
        else if (cqlType instanceof SetType)
        {
            String collectionNamespace = namespace + '.' + cqlType.asCQL3Type().toString();
            result = SchemaBuilder.array()
                                  .items(schemaFrom(((SetType<?>) cqlType).getElementsType(), collectionNamespace));
            AvroSchemas.flagArrayAsSet(result);
        }
        else if (cqlType instanceof MapType)
        {
            String arrayBasedMapName = AvroConstants.ARRAY_BASED_MAP_NAME;
            String childNamespace = namespace + '.' + arrayBasedMapName;
            // create a new avro schema, array of records, to simulate a map.
            SchemaBuilder.FieldAssembler<Schema> keyValue = SchemaBuilder.builder()
                                                                         .record(arrayBasedMapName)
                                                                         .namespace(childNamespace)
                                                                         .fields();
            MapType<?, ?> cqlMap = (MapType<?, ?>) cqlType;
            // map keys and values are not nullable
            keyValue.name(ARRAY_BASED_MAP_KEY_NAME)
                    .type(schemaFrom(cqlMap.getKeysType(), childNamespace))
                    .noDefault();
            keyValue.name(ARRAY_BASED_MAP_VALUE_NAME)
                    .type(schemaFrom(cqlMap.getValuesType(), childNamespace))
                    .noDefault();
            result = SchemaBuilder.builder()
                                  .array()
                                  .items(keyValue.endRecord());
            AvroSchemas.flagArrayAsMap(result);
        }
        else if (cqlType instanceof UserType)
        {
            UserType userType = (UserType) cqlType;
            String recordBasedUdt = AvroConstants.RECORD_BASED_UDT_NAME;
            String childNamespace = namespace + '.' + recordBasedUdt;
            Schema[] ar = userType.fieldTypes().stream().map(udtType -> schemaFrom(udtType, childNamespace)).toArray(Schema[]::new);

            // create a new avro schema, a single record, to represent the udt.
            SchemaBuilder.FieldAssembler<Schema> udtValue = SchemaBuilder.builder()
                                                                         .record(recordBasedUdt)
                                                                         .namespace(childNamespace)
                                                                         .fields();

            for (int i = 0; i < ar.length; i++)
            {
                udtValue.name(userType.fieldNameAsString(i))
                        .type(ar[i])
                        .noDefault();
            }
            result = udtValue.endRecord();
            AvroSchemas.flagAsUdt(result);
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
