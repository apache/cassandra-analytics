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

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class CassandraPartitionReaderFactory implements PartitionReaderFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPartitionReaderFactory.class);
    private static final String CASSANDRA_COLUMNAR_READS_PARAM_NAME = "cassandra.columnar.reads";

    private final CaseInsensitiveStringMap options;
    final DataLayer dataLayer;
    final StructType requiredSchema;
    final List<PartitionKeyFilter> partitionKeyFilters;

    CassandraPartitionReaderFactory(CaseInsensitiveStringMap options,
                                    DataLayer dataLayer,
                                    StructType requiredSchema,
                                    List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.options = options;
        this.dataLayer = dataLayer;
        this.requiredSchema = requiredSchema;
        this.partitionKeyFilters = partitionKeyFilters;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        int partitionId = getPartitionId(partition);
        return new SparkRowIterator(partitionId, dataLayer, requiredSchema, partitionKeyFilters);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition)
    {
        return options.getBoolean(CASSANDRA_COLUMNAR_READS_PARAM_NAME, false);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition)
    {
        int partitionId = getPartitionId(partition);
        return new SparkColumnIterator(options, partitionId, dataLayer, requiredSchema, partitionKeyFilters);
    }

    private int getPartitionId(InputPartition partition)
    {
        int partitionId;
        if (partition instanceof CassandraInputPartition)
        {
            partitionId = ((CassandraInputPartition) partition).getPartitionId();
        }
        else
        {
            partitionId = TaskContext.getPartitionId();
            LOGGER.warn("InputPartition is not of CassandraInputPartition type. "
                                + "Using TaskContext to determine the partitionId type={}, partitionId={}",
                        partition.getClass().getName(), partitionId);
        }

        return partitionId;
    }

}
