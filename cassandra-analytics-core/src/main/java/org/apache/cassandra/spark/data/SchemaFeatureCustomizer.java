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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.sparksql.CellMetadataDecorator;
import org.apache.cassandra.spark.sparksql.LastModifiedTimestampDecorator;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

class SchemaFeatureCustomizer
{
    private SchemaFeatureCustomizer()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    static void aliasLastModifiedTimestamp(List<SchemaFeature> requestedFeatures, String alias)
    {
        int index = requestedFeatures.indexOf(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP);
        if (index >= 0)
        {
            SchemaFeature featureAlias = new SchemaFeature()
            {
                @Override
                public String optionName()
                {
                    return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName();
                }

                @Override
                public String fieldName()
                {
                    return alias;
                }

                @Override
                public DataType fieldDataType()
                {
                    return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.fieldDataType();
                }

                @Override
                public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
                {
                    return new LastModifiedTimestampDecorator<>(builder, alias);
                }

                @Override
                public boolean fieldNullable()
                {
                    return SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.fieldNullable();
                }
            };
            requestedFeatures.set(index, featureAlias);
        }
    }

    static void addCellLastModifiedTimestamp(List<SchemaFeature> requestedFeatures, Map<String, String> columns)
    {
        int index = requestedFeatures.indexOf(SchemaFeatureSet.CELL_LAST_MODIFIED_TIMESTAMP);
        if (index >= 0)
        {
            requestedFeatures.remove(index);
            List<String> sortedColumns = columns.keySet().stream().sorted().collect(Collectors.toList());
            for (String column : sortedColumns)
            {
                SchemaFeature lastModifiedTimestampFeature = new SchemaFeature()
                {
                    @Override
                    public String optionName()
                    {
                        return SchemaFeatureSet.CELL_LAST_MODIFIED_TIMESTAMP.optionName();
                    }

                    @Override
                    public String fieldName()
                    {
                        return columns.get(column);
                    }

                    @Override
                    public DataType fieldDataType()
                    {
                        return DataTypes.TimestampType;
                    }

                    @Override
                    public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
                    {
                        CqlField source = findTtlAndTimestampAwareCqlField(builder.getCqlTable(), column, optionName());
                        return new CellMetadataDecorator<>(builder, source.position(), fieldName(), cell -> cell.timestamp);
                    }
                };
                requestedFeatures.add(lastModifiedTimestampFeature);
            }
        }
    }

    static void addCellTtl(List<SchemaFeature> requestedFeatures, Map<String, String> columns)
    {
        int index = requestedFeatures.indexOf(SchemaFeatureSet.CELL_TTL);
        if (index >= 0)
        {
            requestedFeatures.remove(index);
            List<String> sortedColumns = columns.keySet().stream().sorted().collect(Collectors.toList());
            for (String column : sortedColumns)
            {
                SchemaFeature lastModifiedTimestampFeature = new SchemaFeature()
                {
                    @Override
                    public String optionName()
                    {
                        return SchemaFeatureSet.CELL_TTL.optionName();
                    }

                    @Override
                    public String fieldName()
                    {
                        return columns.get(column);
                    }

                    @Override
                    public DataType fieldDataType()
                    {
                        return DataTypes.IntegerType;
                    }

                    @Override
                    public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
                    {
                        CqlField source = findTtlAndTimestampAwareCqlField(builder.getCqlTable(), column, optionName());
                        return new CellMetadataDecorator<>(builder, source.position(), fieldName(),
                                                           cell -> cell.ttl == CqlField.NO_TTL ? null : cell.ttl);
                    }
                };
                requestedFeatures.add(lastModifiedTimestampFeature);
            }
        }
    }

    @VisibleForTesting
    static CqlField findTtlAndTimestampAwareCqlField(CqlTable table, String sourceColumn, String optionName)
    {
        // Prefer an exact match first. This is important for quoted identifiers.
        CqlField source = table.getField(sourceColumn);

        if (source == null)
        {
            // Spark options are case-insensitive, so the column suffix may have
            // lost its original case. Fall back to case-insensitive resolution.
            List<CqlField> matches = table.fields()
                                          .stream()
                                          .filter(field -> field.name().equalsIgnoreCase(sourceColumn))
                                          .collect(Collectors.toList());
            Preconditions.checkArgument(!matches.isEmpty(),
                                        "Unable to enable schema feature '%s': "
                                        + "column '%s' does not exist in table %s.%s",
                                        optionName,
                                        sourceColumn,
                                        table.keyspace(),
                                        table.table());
            Preconditions.checkArgument(matches.size() == 1,
                                        "Unable to enable schema feature '%s': "
                                        + "column '%s' is ambiguous in table %s.%s. "
                                        + "Matching columns: %s",
                                        optionName,
                                        sourceColumn,
                                        table.keyspace(),
                                        table.table(),
                                        matches.stream()
                                               .map(CqlField::name)
                                               .collect(Collectors.joining(", ")));
            source = matches.get(0);
        }

        Preconditions.checkArgument(!source.isPrimaryKey(),
                                    "Unable to enable schema feature '%s': "
                                    + "column '%s' is part of primary key",
                                    optionName,
                                    source.name());
        return source;
    }
}
