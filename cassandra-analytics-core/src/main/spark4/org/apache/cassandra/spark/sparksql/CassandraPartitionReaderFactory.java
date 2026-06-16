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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

class CassandraPartitionReaderFactory implements PartitionReaderFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPartitionReaderFactory.class);
    final DataLayer dataLayer;
    final StructType requiredSchema;
    final List<PartitionKeyFilter> partitionKeyFilters;

    CassandraPartitionReaderFactory(DataLayer dataLayer,
                                    StructType requiredSchema,
                                    List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.dataLayer = dataLayer;
        this.requiredSchema = requiredSchema;
        this.partitionKeyFilters = partitionKeyFilters;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
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
        return new SparkRowIterator(partitionId, dataLayer, requiredSchema, partitionKeyFilters);
    }
}
