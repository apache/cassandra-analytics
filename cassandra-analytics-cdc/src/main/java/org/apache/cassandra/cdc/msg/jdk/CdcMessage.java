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

package org.apache.cassandra.cdc.msg.jdk;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.bridge.CassandraTypesImplementation;
import org.apache.cassandra.cdc.msg.AbstractCdcEvent;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.complex.CqlCollection;
import org.apache.cassandra.spark.utils.ArrayUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.ArrayUtils.orElse;

/**
 * Converts `org.apache.cassandra.cdc.msg.jdk.CdcEvent` into more user-consumable format, deserializing ByteBuffers into Java types.
 */
@SuppressWarnings("unused")
public class CdcMessage
{
    private final String keyspace;
    private final String table;
    private final List<Column> partitionKeys;
    private final List<Column> clusteringKeys;
    private final List<Column> staticColumns;
    private final List<Column> valueColumns;
    private final long maxTimestampMicros;
    private final AbstractCdcEvent.Kind operationType;
    private final Map<String, Column> columns;
    private final List<RangeTombstoneMsg> rangeTombstoneList;
    @Nullable
    private final AbstractCdcEvent.TimeToLive ttl;
    @Nullable
    private final Map<String, List<Object>> complexCellDeletion;

    public CdcMessage(CdcEvent event)
    {
        this(event.keyspace,
             event.table,
             toColumns(event.getPartitionKeys()),
             toColumns(event.getClusteringKeys()),
             toColumns(event.getStaticColumns()),
             toColumns(event.getValueColumns()),
             event.getTimestamp(TimeUnit.MICROSECONDS),
             event.getKind(),
             orElse(event.getRangeTombstoneList(), ImmutableList.of()).stream().map(RangeTombstone::toCdcMessage).collect(Collectors.toList()),
             complexCellDeletion(event.getTombstonedCellsInComplex(), typeProvider(event)),
             event.getTtl());
    }

    @SuppressWarnings("unchecked")
    private static Function<String, CqlField.CqlType> typeProvider(CdcEvent event)
    {
        final List<Value> cols = ArrayUtils.combine(event.getPartitionKeys(),
                                                    event.getClusteringKeys(),
                                                    event.getStaticColumns(),
                                                    event.getValueColumns());
        final Map<String, CqlField.CqlType> typeMap = cols
                                                      .stream()
                                                      .collect(Collectors
                                                               .toMap(v -> v.columnName,
                                                                      v -> CassandraTypesImplementation.INSTANCE.parseType(event.keyspace, v.columnType)
                                                               ));
        return typeMap::get;
    }

    public CdcMessage(String keyspace,
                      String table,
                      List<Column> partitionKeys,
                      List<Column> clusteringKeys,
                      List<Column> staticColumns,
                      List<Column> valueColumns,
                      long maxTimestampMicros,
                      AbstractCdcEvent.Kind operationType,
                      List<RangeTombstoneMsg> rangeTombstoneList,
                      @Nullable Map<String, List<Object>> complexCellDeletion,
                      @Nullable AbstractCdcEvent.TimeToLive ttl)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.staticColumns = staticColumns;
        this.valueColumns = valueColumns;
        this.maxTimestampMicros = maxTimestampMicros;
        this.operationType = operationType;
        this.rangeTombstoneList = rangeTombstoneList;
        this.columns = new HashMap<>(partitionKeys.size() + clusteringKeys.size() + staticColumns.size() + valueColumns.size());
        partitionKeys.forEach(col -> this.columns.put(col.name(), col));
        clusteringKeys.forEach(col -> this.columns.put(col.name(), col));
        staticColumns.forEach(col -> this.columns.put(col.name(), col));
        valueColumns.forEach(col -> this.columns.put(col.name(), col));
        this.complexCellDeletion = complexCellDeletion;
        this.ttl = ttl;
    }

    private static Map<String, List<Object>> complexCellDeletion(@Nullable Map<String, List<ByteBuffer>> tombstonedCellsInComplex,
                                                                 Function<String, CqlField.CqlType> typeProvider)
    {
        if (tombstonedCellsInComplex == null)
        {
            return null;
        }

        return tombstonedCellsInComplex
               .entrySet()
               .stream()
               .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                   final CqlCollection type = (CqlCollection) typeProvider.apply(entry.getKey());
                   return entry.getValue().stream().map(ByteBuffer::duplicate).map(buf -> type.type().deserializeToJavaType(buf)).collect(Collectors.toList());
               }));
    }

    private static List<Column> toColumns(List<Value> values)
    {
        if (values == null)
        {
            return ImmutableList.of();
        }
        return values.stream()
                     .map(Value::toCdcMessage)
                     .collect(Collectors.toList());
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public List<Column> partitionKeys()
    {
        return partitionKeys;
    }

    public List<Column> clusteringKeys()
    {
        return clusteringKeys;
    }

    public List<Column> staticColumns()
    {
        return staticColumns;
    }

    public List<Column> valueColumns()
    {
        return valueColumns;
    }

    @Nullable
    public AbstractCdcEvent.TimeToLive ttl()
    {
        return ttl;
    }

    @Nullable
    public Map<String, List<Object>> getComplexCellDeletion()
    {
        return complexCellDeletion;
    }

    public List<RangeTombstoneMsg> rangeTombstones()
    {
        return rangeTombstoneList;
    }

    public long lastModifiedTimeMicros()
    {
        return maxTimestampMicros;
    }

    public AbstractCdcEvent.Kind operationType()
    {
        return operationType;
    }

    // convenience apis

    @SuppressWarnings("unchecked")
    public List<Column> primaryKeys()
    {
        return ArrayUtils.combine(partitionKeys, clusteringKeys);
    }

    @SuppressWarnings("unchecked")
    public List<Column> allColumns()
    {
        return ArrayUtils.combine(partitionKeys, clusteringKeys, staticColumns, valueColumns);
    }

    @Nullable
    public Column column(String name)
    {
        return columns.get(name);
    }

    public Instant lastModifiedTime()
    {
        return Instant.EPOCH.plus(maxTimestampMicros, ChronoUnit.MICROS);
    }

    @SuppressWarnings("unchecked")
    public String toString()
    {
        return '{' +
               "\"operation\": " + operationType + ", " +
               "\"lastModifiedTimestamp\": " + maxTimestampMicros + ", " +
               ArrayUtils.concatToStream(partitionKeys, clusteringKeys, staticColumns, valueColumns)
                         .map(Object::toString)
                         .collect(Collectors.joining(", ")) +
               '}';
    }
}
