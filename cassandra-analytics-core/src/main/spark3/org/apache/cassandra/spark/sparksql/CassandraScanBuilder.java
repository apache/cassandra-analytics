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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class CassandraScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportPartitioning
{
    final DataLayer dataLayer;
    final StructType schema;
    final CaseInsensitiveStringMap options;
    StructType requiredSchema = null;
    Filter[] pushedFilters = new Filter[0];

    CassandraScanBuilder(DataLayer dataLayer, StructType schema, CaseInsensitiveStringMap options)
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
    public void pruneColumns(StructType requiredSchema)
    {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters)
    {
        Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(filters);

        List<Filter> supportedFilters = new ArrayList<>(Arrays.asList(filters));
        supportedFilters.removeAll(Arrays.asList(unsupportedFilters));
        pushedFilters = supportedFilters.toArray(new Filter[0]);

        return unsupportedFilters;
    }

    @Override
    public Filter[] pushedFilters()
    {
        return pushedFilters;
    }

    @Override
    public StructType readSchema()
    {
        return requiredSchema;
    }

    @Override
    public Batch toBatch()
    {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        return IntStream.range(0, dataLayer.partitionCount())
                        .mapToObj(CassandraInputPartition::new)
                        .toArray(InputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return new CassandraPartitionReaderFactory(dataLayer, requiredSchema, buildPartitionKeyFilters());
    }

    @Override
    public Partitioning outputPartitioning()
    {
        return new CassandraPartitioning(dataLayer);
    }

    private List<PartitionKeyFilter> buildPartitionKeyFilters()
    {
        List<String> partitionKeyColumnNames = dataLayer.cqlTable().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
        Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(pushedFilters, new HashSet<>(partitionKeyColumnNames));

        if (partitionKeyValues.isEmpty())
        {
            return Collections.emptyList();
        }

        List<List<String>> orderedValues = partitionKeyColumnNames.stream().map(partitionKeyValues::get).collect(Collectors.toList());
        return FilterUtils.cartesianProduct(orderedValues).stream()
                          .map(this::buildFilter)
                          .collect(Collectors.toList());
    }

    private PartitionKeyFilter buildFilter(List<String> keys)
    {
        AbstractMap.SimpleEntry<ByteBuffer, BigInteger> filterKey = dataLayer.bridge().getPartitionKey(dataLayer.cqlTable(), dataLayer.partitioner(), keys);
        return PartitionKeyFilter.create(filterKey.getKey(), filterKey.getValue());
    }
}
