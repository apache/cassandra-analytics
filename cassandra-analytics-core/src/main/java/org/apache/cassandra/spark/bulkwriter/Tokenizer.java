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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.bulkwriter.token.TokenUtils;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

public class Tokenizer implements Serializable
{

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable
    {
    }
    private final TokenUtils tokenUtils;
    private final SerializableFunction<Object[], Object[]> keyColumnProvider;

    public Tokenizer(BulkWriterContext writerContext)
    {
        TableSchema tableSchema = writerContext.schema().getTableSchema();
        this.tokenUtils = new TokenUtils(tableSchema.partitionKeyColumns,
                                         tableSchema.partitionKeyColumnTypes,
                                         writerContext.cluster().getPartitioner() == Partitioner.Murmur3Partitioner);
        this.keyColumnProvider = tableSchema::getKeyColumns;
    }

    @VisibleForTesting
    public Tokenizer(List<Integer> keyColumnIndexes,
                     List<String> partitionKeyColumns,
                     List<ColumnType<?>> partitionKeyColumnTypes,
                     boolean isMurmur3Partitioner)
    {
        this.keyColumnProvider = (columns) -> TableSchema.getKeyColumns(columns, keyColumnIndexes);
        this.tokenUtils = new TokenUtils(partitionKeyColumns,
                                         partitionKeyColumnTypes,
                                         isMurmur3Partitioner);
    }

    public DecoratedKey getDecoratedKey(Object[] columns)
    {
        Object[] keyColumns = keyColumnProvider.apply(columns);
        ByteBuffer key = tokenUtils.getCompositeKey(keyColumns);
        return new DecoratedKey(tokenUtils.getToken(key), key);
    }
}
