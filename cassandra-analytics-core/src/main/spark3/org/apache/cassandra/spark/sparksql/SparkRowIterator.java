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

package org.apache.cassandra.spark.sparksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper iterator around SparkCellIterator to normalize cells into Spark SQL rows
 */
public class SparkRowIterator extends AbstractSparkRowIterator implements PartitionReader<InternalRow>
{
    @VisibleForTesting
    public SparkRowIterator(int partitionId, @NotNull DataLayer dataLayer)
    {
        this(partitionId, dataLayer, null, new ArrayList<>(), null);
    }

    public SparkRowIterator(int partitionId,
                            @NotNull DataLayer dataLayer,
                            @Nullable StructType columnFilter,
                            @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        this(partitionId, dataLayer, columnFilter, partitionKeyFilters, null);
    }

    protected SparkRowIterator(int partitionId,
                               @NotNull DataLayer dataLayer,
                               @Nullable StructType columnFilter,
                               @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                               @Nullable CdcOffsetFilter cdcOffsetFilter)
    {
        super(partitionId, dataLayer, columnFilter, partitionKeyFilters, cdcOffsetFilter);
    }

    @Override
    @NotNull
    RowBuilder newBuilder()
    {
        RowBuilder builder;
        String[] fieldNames = null;
        if (columnFilter != null)
        {
            builder = new PartialRowBuilder(columnFilter, cqlTable, noValueColumns);
            fieldNames = columnFilter.fieldNames();
        }
        else
        {
            builder = new FullRowBuilder(cqlTable, noValueColumns);
        }

        for (SchemaFeature feature : requestedFeatures)
        {
            // Only decorate when there is no column filter or when the field is requested in the query,
            // otherwise we skip decoration
            if (columnFilter == null || Arrays.stream(fieldNames).anyMatch(feature.fieldName()::equals))
            {
                builder = feature.decorate(builder);
            }
        }

        builder.reset();
        return builder;
    }

    /**
     * PartialRowBuilder that builds row only containing fields in requiredSchema prune-column filter
     * NOTE: Spark 3 changed the contract from Spark 2 and requires us to only return the columns specified in
     * the requiredSchema 'prune column' filter and not a sparse Object[] array with null values for excluded columns
     */
    static class PartialRowBuilder extends FullRowBuilder
    {
        private final int[] positionsMap;
        private final boolean hasAllNonValueColumns;
        private final StructType requiredSchema;

        PartialRowBuilder(@NotNull StructType requiredSchema,
                          CqlTable table,
                          boolean noValueColumns)
        {
            super(table, noValueColumns);
            this.requiredSchema = requiredSchema;
            Set<String> requiredColumns = Arrays.stream(requiredSchema.fields())
                                                .map(StructField::name)
                                                .collect(Collectors.toSet());
            hasAllNonValueColumns = table.fields().stream()
                                                   .filter(CqlField::isNonValueColumn)
                                                   .map(CqlField::name)
                                                   .allMatch(requiredColumns::contains);

            // Map original column position to new position in requiredSchema
            positionsMap = IntStream.range(0, table.numFields())
                                    .map(position -> -1)
                                    .toArray();
            int position = 0;
            for (StructField structField : requiredSchema.fields())
            {
                CqlField field = table.getField(structField.name());
                if (field != null)  // Field might be last modified timestamp
                {
                    positionsMap[field.position()] = position++;
                }
            }
        }

        @Override
        public void reset()
        {
            count = 0;
            int totalColumns = requiredSchema.size();
            if (totalColumns > 0)
            {
                result = new Object[totalColumns];
            }
            else
            {
                result = EMPTY_RESULT;
            }
        }

        @Override
        public int fieldIndex(String name)
        {
            return requiredSchema != null ? requiredSchema.fieldIndex(name) : super.fieldIndex(name);
        }

        @Override
        public void copyKeys(Cell cell)
        {
            if (hasAllNonValueColumns)
            {
                // Optimization if we are returning all primary key/static columns we can use the super method
                super.copyKeys(cell);
                return;
            }

            // Otherwise we need to only return columns requested and map to new position in result array
            int length = noValueColumns || cell.isTombstone() ? cell.values.length : cell.values.length - 1;
            for (int index = 0; index < length; index++)
            {
                int position = positionsMap[index];
                if (position >= 0)
                {
                    result[position] = cell.values[index];
                }
            }
            count += length;
        }

        @Override
        public void copyValue(Cell cell)
        {
            // Copy the next value column mapping column to new position
            int position = positionsMap[cell.position];
            if (position >= 0)
            {
                result[position] = cell.values[cell.values.length - 1];
            }
            count++;
        }
    }
}
