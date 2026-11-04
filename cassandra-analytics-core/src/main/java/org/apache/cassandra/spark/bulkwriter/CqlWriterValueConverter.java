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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.bridge.CanonicalValueConverter;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.BridgeUdtValue;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

/**
 * Converts the values of a row normalized by {@link SqlToCqlTypeConverter} into the form that the bridge's
 * {@link org.apache.cassandra.bridge.SSTableWriter} accepts. {@link BridgeUdtValue} instances become the bridge's
 * UDT values and tuples, which are normalized to {@code Object[]}, become the bridge's tuple values. The bridge's
 * {@link CanonicalValueConverter} then sorts the sets and maps nested in the values, so that the writer stores them
 * as Cassandra stores their CQL literals.
 * <p>
 * {@link RecordWriter} converts every column of a row before writing it, and {@link Tokenizer} converts the
 * partition key columns before computing the token of the row. Both must convert in the same way, so that the
 * token computed for a row is the token of the partition key that the SSTable writer derives from it.
 * <p>
 * The input value is never modified. {@link Tokenizer} converts a row before Spark shuffles it, and the shuffled
 * row must keep values that Spark can serialize.
 */
class CqlWriterValueConverter
{
    private final CqlTable cqlTable;
    private final CassandraVersion version;
    private final CanonicalValueConverter canonicalValueConverter;
    private final Map<String, CqlField.CqlUdt> udtCache = new ConcurrentHashMap<>();

    /**
     * @param cqlTable    table being written, built by {@code bridge}
     * @param bridge      bridge whose SSTable writer writes the values
     * @param partitioner partitioner of the cluster being written to
     */
    CqlWriterValueConverter(@NotNull CqlTable cqlTable, @NotNull CassandraBridge bridge, @NotNull Partitioner partitioner)
    {
        this.cqlTable = cqlTable;
        this.version = bridge.getVersion();
        this.canonicalValueConverter = bridge.canonicalValueConverter(cqlTable, partitioner);
    }

    /**
     * @param columnName name of the column the value belongs to
     * @param value      value normalized by {@link SqlToCqlTypeConverter}
     * @return the value in the form that the bridge's SSTable writer accepts for the column
     */
    Object convert(String columnName, Object value)
    {
        Object columnValue = value;

        // convert all BridgeUDTValue types in the column to UDTValue
        if (cqlTable.containsUdt(columnName))
        {
            columnValue = maybeConvertUdt(columnValue);
        }

        // Convert tuples to TupleValue for CQLSSTableWriter
        // - Direct tuple columns: Object[] -&gt; TupleValue
        // - Collections with tuples: List<Object[]> -&gt; List<TupleValue> (via collection's convertForCqlWriter)
        CqlField field = cqlTable.getField(columnName);
        if (field != null && columnValue != null)
        {
            CqlField.CqlType fieldType = field.type();

            // Check if this is a direct tuple
            if (CqlTable.isTupleType(fieldType))
            {
                CqlField.CqlTuple cqlTuple = (CqlField.CqlTuple) CqlTable.unwrapIfFrozen(fieldType);
                columnValue = cqlTuple.convertForCqlWriter(columnValue, version, false);
            }
            // Check if this is a collection that contains tuples
            else if (CqlTable.containsTuples(fieldType))
            {
                CqlField.CqlType collectionType = CqlTable.unwrapIfFrozen(fieldType);
                if (collectionType instanceof CqlField.CqlCollection)
                {
                    columnValue = ((CqlField.CqlCollection) collectionType).convertForCqlWriter(columnValue, version, false);
                }
            }
        }
        return canonicalValueConverter.convert(columnName, columnValue);
    }

    /**
     * A column can have UDTs somewhere nested inside collections/UDTs. All occurrences of BridgeUdtValue need to be
     * recursively converted to UDTValue to be able to write to CQL.
     * @param value column value
     * @return column value after converting all occurrences of BridgeUdtValue to UDTValue
     */
    private Object maybeConvertUdt(Object value)
    {
        if (value instanceof List && !((List<?>) value).isEmpty())
        {
            List<Object> resultList = new ArrayList<>();
            for (Object entry : (List<?>) value)
            {
                resultList.add(maybeConvertUdt(entry));
            }

            return resultList;
        }

        // Tuples come here as Object[]
        if (value instanceof Object[])
        {
            Object[] valueArray = (Object[]) value;
            Object[] resultArray = new Object[valueArray.length];
            for (int i = 0; i < valueArray.length; i++)
            {
                resultArray[i] = maybeConvertUdt(valueArray[i]);
            }

            return resultArray;
        }

        if (value instanceof Set && !((Set<?>) value).isEmpty())
        {
            Set<Object> resultSet = new HashSet<>();
            for (Object entry : (Set<?>) value)
            {
                resultSet.add(maybeConvertUdt(entry));
            }

            return resultSet;
        }

        if (value instanceof Map && !((Map<?, ?>) value).isEmpty())
        {
            Map<Object, Object> resultMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
            {
                resultMap.put(maybeConvertUdt(entry.getKey()), maybeConvertUdt(entry.getValue()));
            }

            return resultMap;
        }

        if (value instanceof BridgeUdtValue)
        {
            BridgeUdtValue udtValue = (BridgeUdtValue) value;
            // Depth-first replacement of BridgeUdtValue instances to their appropriate Cql types.
            // The fields are converted into a new map so that the input value is left unchanged.
            Map<String, Object> convertedFields = new HashMap<>();
            for (Map.Entry<String, Object> entry : udtValue.udtMap.entrySet())
            {
                // udt can have complex types like nested udt, list, set or map with embedded UDTs in them
                // convert each entry recursively until we see basic datatype
                convertedFields.put(entry.getKey(), maybeConvertUdt(entry.getValue()));
            }
            return getUdt(udtValue.name).convertForCqlWriter(convertedFields, version, false);
        }

        return value;
    }

    private CqlField.CqlType getUdt(String udtName)
    {
        return udtCache.computeIfAbsent(udtName, name -> {
            for (CqlField.CqlUdt udt : cqlTable.udts())
            {
                if (udt.cqlName().equals(name))
                {
                    return udt;
                }
            }
            throw new IllegalArgumentException("Could not find udt with name " + name);
        });
    }
}
