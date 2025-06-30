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

import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.cassandra.spark.utils.Preconditions;
import javax.annotation.Nullable;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

public class AvroSchemas
{
    private AvroSchemas()
    {
    }

    private static final LogicalType ARRAY_MAP_LOGICAL_TYPE = new LogicalType(AvroConstants.ARRAY_BASED_MAP_NAME);
    private static final LogicalType ARRAY_SET_LOGICAL_TYPE = new LogicalType(AvroConstants.ARRAY_BASED_SET_NAME);
    private static final LogicalType UDT_LOGICAL_TYPE = new LogicalType(AvroConstants.RECORD_BASED_UDT_NAME);
    private static final LogicalType INET_LOGICAL_TYPE = new LogicalType(AvroConstants.INET_NAME);
    private static final LogicalType VARIABLE_INTEGER_LOGICAL_TYPE = new LogicalType(AvroConstants.VARIABLE_INTEGER_NAME);

    public static void registerLogicalTypes()
    {
        LogicalTypes.register(AvroConstants.ARRAY_BASED_MAP_NAME, schema -> ARRAY_MAP_LOGICAL_TYPE);
        LogicalTypes.register(AvroConstants.ARRAY_BASED_SET_NAME, schema -> ARRAY_SET_LOGICAL_TYPE);
        LogicalTypes.register(AvroConstants.RECORD_BASED_UDT_NAME, schema -> UDT_LOGICAL_TYPE);
        LogicalTypes.register(AvroConstants.INET_NAME, schema -> INET_LOGICAL_TYPE);
        LogicalTypes.register(AvroConstants.VARIABLE_INTEGER_NAME, schema -> VARIABLE_INTEGER_LOGICAL_TYPE);
    }

    public static boolean isArrayBasedMap(Schema schema)
    {
        if (schema.getType() == ARRAY)
        {
            LogicalType logicalType = schema.getLogicalType();
            return logicalType != null && logicalType.getName().equals(AvroConstants.ARRAY_BASED_MAP_NAME);
        }

        return false;
    }

    public static void flagArrayAsMap(Schema schema)
    {
        Preconditions.checkArgument(schema.getType() == ARRAY);
        new LogicalType(AvroConstants.ARRAY_BASED_MAP_NAME).addToSchema(schema);
    }

    public static void flagAsUdt(Schema schema)
    {
        Preconditions.checkArgument(schema.getType() == RECORD);
        new LogicalType(AvroConstants.RECORD_BASED_UDT_NAME).addToSchema(schema);
    }

    public static boolean isRecordBasedUdt(Schema schema)
    {
        if (schema.getType() == RECORD)
        {
            LogicalType logicalType = schema.getLogicalType();
            return logicalType != null && logicalType.getName().equals(AvroConstants.RECORD_BASED_UDT_NAME);
        }

        return false;
    }

    public static void setPrimaryKeys(Schema schema, List<String> primaryKeys)
    {
        schema.addProp(AvroConstants.PRIMARY_KEYS_PROP, primaryKeys);
    }

    public static List<String> primaryKeys(Schema schema)
    {
        return (List<String>) schema.getObjectProp(AvroConstants.PRIMARY_KEYS_PROP);
    }

    public static void setPartitionKeys(Schema schema, List<String> partitionKeys)
    {
        schema.addProp(AvroConstants.PARTITION_KEYS_PROP, partitionKeys);
    }

    public static List<String> partitionKeys(Schema schema)
    {
        return (List<String>) schema.getObjectProp(AvroConstants.PARTITION_KEYS_PROP);
    }

    public static void setClusteringKeys(Schema schema, List<String> clusteringKeys)
    {
        schema.addProp(AvroConstants.CLUSTERING_KEYS_PROP, clusteringKeys);
    }

    public static List<String> clusteringKeys(Schema schema)
    {
        return (List<String>) schema.getObjectProp(AvroConstants.CLUSTERING_KEYS_PROP);
    }

    public static void setStaticColumns(Schema schema, List<String> staticColumns)
    {
        schema.addProp(AvroConstants.STATIC_COLUMNS_PROP, staticColumns);
    }

    public static List<String> staticColumns(Schema schema)
    {
        return (List<String>) schema.getObjectProp(AvroConstants.STATIC_COLUMNS_PROP);
    }

    public static void flagCqlType(Schema schema, String cqlType)
    {
        schema.addProp(AvroConstants.CQL_TYPE_NAME, cqlType);
    }

    /**
     * Read the cqlType of the schema element.
     *
     * @param schema
     * @return cqlType string; if no cqlType property (key: 'cqlType') is defined, returns null.
     */
    @Nullable
    public static String cqlType(Schema schema)
    {
        return schema.getProp(AvroConstants.CQL_TYPE_NAME);
    }

    public static boolean isFrozen(Schema schema)
    {
        String flag = schema.getProp(AvroConstants.FROZEN_FLAG_PROP);
        return Boolean.parseBoolean(flag);
    }

    public static void flagFrozen(Schema schema)
    {
        schema.addProp(AvroConstants.FROZEN_FLAG_PROP, "true");
    }

    public static void flagReversed(Schema schema)
    {
        schema.addProp(AvroConstants.IS_REVERSED, "true");
    }

    public static boolean isArrayBasedSet(Schema schema)
    {
        LogicalType logicalType = schema.getLogicalType();
        return logicalType != null && logicalType.getName().equals(AvroConstants.ARRAY_BASED_SET_NAME);
    }

    public static void flagArrayAsSet(Schema schema)
    {
        new LogicalType(AvroConstants.ARRAY_BASED_SET_NAME).addToSchema(schema);
    }

    /**
     * We generate nullable fields in the avro schema, see CqlToAvroSchemaConverter
     * In this method, we unwrap the nullable fields to get the actual type.
     */
    public static Schema unwrapNullable(Schema schema)
    {
        // a nullable field is a union of extactly 2 types, the actul type and NULL
        if (schema.getType() != UNION || schema.getTypes().size() != 2)
        {
            return schema;
        }

        boolean hasNull = false;
        Schema actual = null;
        for (Schema s : schema.getTypes())
        {
            if (s.getType() == NULL)
            {
                hasNull = true;
            }
            else
            {
                actual = s;
            }
        }

        // if the schema types has null and has an actual type, we are sure that it is a nullable type,
        // extract the actual type and return; otherwise, we should return the input schema as-is.
        return hasNull && actual != null
               ? actual : schema;
    }
}
