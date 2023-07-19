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

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public abstract class PartitionSizeTableProvider extends CassandraTableProvider
{
    public abstract DataLayer getDataLayer(CaseInsensitiveStringMap options);

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        return getDataLayerInternal(options).partitionSizeStructType();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        return new PartitionSizeTable(getDataLayerInternal(new CaseInsensitiveStringMap(properties)), schema);
    }
}

class PartitionSizeTable implements Table, SupportsRead
{
    private final DataLayer dataLayer;
    private final StructType schema;

    PartitionSizeTable(DataLayer dataLayer, StructType schema)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
    }

    @Override
    public String name()
    {
        return dataLayer.cqlTable().keyspace() + "." + dataLayer.cqlTable().table();
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return ImmutableSet.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new PartitionSizeScanBuilder(dataLayer, schema, options);
    }
}

class PartitionSizeScanBuilder implements ScanBuilder, Scan, Batch
{
    final DataLayer dataLayer;
    final StructType schema;
    final CaseInsensitiveStringMap options;

    PartitionSizeScanBuilder(DataLayer dataLayer, StructType schema, CaseInsensitiveStringMap options)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build()
    {
        return this;
    }

    @Override
    public StructType readSchema()
    {
        return schema;
    }

    @Override
    public Batch toBatch()
    {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        return IntStream.range(0, this.dataLayer.partitionCount())
                        .mapToObj(CassandraInputPartition::new)
                        .toArray(InputPartition[]::new);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation)
    {
        return new CassandraMicroBatchStream(dataLayer, null, options);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return new PartitionSizeReaderFactory(dataLayer);
    }
}

class PartitionSizeReaderFactory implements PartitionReaderFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPartitionReaderFactory.class);
    final DataLayer dataLayer;

    PartitionSizeReaderFactory(DataLayer dataLayer)
    {
        this.dataLayer = dataLayer;
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
            LOGGER.warn("InputPartition is not of CassandraInputPartition type. Using TaskContext to determine the partitionId type={}, partitionId={}",
                        partition.getClass().getName(), partitionId);
        }
        return new PartitionSizeIterator(partitionId, dataLayer);
    }
}
