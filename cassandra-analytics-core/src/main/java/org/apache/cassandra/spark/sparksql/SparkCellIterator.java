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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.utils.FastThreadLocalUtf8Decoder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SparkCellIterator extends CellIterator
{
    private final DataLayer dataLayer;
    private final SparkType[] sparkTypes;

    public SparkCellIterator(int partitionId,
                             @NotNull DataLayer dataLayer,
                             @Nullable StructType requiredSchema,
                             @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        super(partitionId,
              dataLayer.cqlTable(),
              dataLayer.stats(),
              dataLayer.typeConverter(),
              partitionKeyFilters,
              (cqlTable) -> buildColumnFilter(requiredSchema, cqlTable),
              dataLayer::openCompactionScanner);
        this.dataLayer = dataLayer;
        this.sparkTypes = new SparkType[cqlTable.numFields()];
        SparkSqlTypeConverter sparkSqlTypeConverter = ((SparkSqlTypeConverter) this.typeConverter);
        for (int index = 0; index < cqlTable.numFields(); index++)
        {
            this.sparkTypes[index] = sparkSqlTypeConverter.toSparkType(cqlTable.field(index).type());
        }
    }

    @Nullable
    static PruneColumnFilter buildColumnFilter(@Nullable StructType requiredSchema, @NotNull CqlTable cqlTable)
    {
        return requiredSchema != null
               ? new PruneColumnFilter(Arrays.stream(requiredSchema.fields())
                                             .map(StructField::name)
                                             .filter(cqlTable::has)
                                             .collect(Collectors.toSet()))
               : null;
    }

    @Override
    public boolean isInPartition(int partitionId, BigInteger token, ByteBuffer partitionKey)
    {
        return dataLayer.isInPartition(partitionId, token, partitionKey);
    }

    @Override
    public boolean equals(CqlField field, Object obj1, Object obj2)
    {
        return this.sparkTypes[field.position()].equals(obj1, obj2);
    }

    @Override
    protected String decodeString(@Nullable ByteBuffer buffer)
    {
        return buffer != null ? FastThreadLocalUtf8Decoder.stringThrowRuntime(buffer) : null;
    }
}
