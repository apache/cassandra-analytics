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
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.S3CassandraDataLayer;
import org.apache.cassandra.spark.data.SSTableTokenIndex;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.metrics.TotalSummaryReadDuration;
import org.apache.cassandra.spark.sparksql.metrics.TotalOpenedSSTableDuration;
import org.apache.cassandra.spark.sparksql.metrics.TotalCorruptSSTableCount;
import org.apache.cassandra.spark.sparksql.metrics.TotalMutableMetadataDriftCount;
import org.apache.cassandra.spark.sparksql.metrics.TotalMutableMetadataHeadFallbackCount;
import org.apache.cassandra.spark.sparksql.metrics.TotalSkippedSSTableCount;
import org.apache.cassandra.spark.sparksql.metrics.TotalS3HeadObjectDuration;
import org.apache.cassandra.spark.sparksql.metrics.TotalS3GetObjectDuration;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.broadcast.Broadcast;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CassandraScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportPartitioning,
                                      SupportsReportStatistics
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScanBuilder.class);

    final DataLayer dataLayer;
    final StructType schema;
    final CaseInsensitiveStringMap options;
    StructType requiredSchema = null;
    Filter[] pushedFilters = new Filter[0];
    @Nullable
    private final Broadcast<SSTableTokenIndex> sstableTokenIndexBroadcast;

    CassandraScanBuilder(DataLayer dataLayer, StructType schema, CaseInsensitiveStringMap options)
    {
        this(dataLayer, schema, options, null);
    }

    CassandraScanBuilder(DataLayer dataLayer,
                         StructType schema,
                         CaseInsensitiveStringMap options,
                         @Nullable Broadcast<SSTableTokenIndex> sstableTokenIndexBroadcast)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
        this.options = options;
        this.sstableTokenIndexBroadcast = sstableTokenIndexBroadcast;
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
        return requiredSchema == null ? schema : requiredSchema;
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
        if (sstableTokenIndexBroadcast == null && dataLayer instanceof S3CassandraDataLayer
            && ((S3CassandraDataLayer) dataLayer).sstableTokenIndexEnabled())
        {
            LOGGER.warn("SSTable token index is enabled but no prebuilt read context was provided. "
                        + "Proceeding without token-index pruning.");
        }
        return new CassandraPartitionReaderFactory(dataLayer, readSchema(), buildPartitionKeyFilters(), sstableTokenIndexBroadcast);
    }

    @Override
    public Partitioning outputPartitioning()
    {
        // See CassandraPartitioning for why we report UnknownPartitioning (token-range Spark
        // partitions are not key-grouped by Cassandra partition key, so KeyGroupedPartitioning
        // is the wrong contract here).
        return new CassandraPartitioning(dataLayer.partitionCount());
    }

    private List<PartitionKeyFilter> buildPartitionKeyFilters()
    {
        List<String> partitionKeyColumnNames = dataLayer.cqlTable().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
        Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(pushedFilters, new HashSet<>(partitionKeyColumnNames));
        if (partitionKeyValues.size() > 0)
        {
            List<List<String>> orderedValues = partitionKeyColumnNames.stream().map(partitionKeyValues::get).collect(Collectors.toList());
            return FilterUtils.cartesianProduct(orderedValues).stream()
                .map(this::buildFilter)
                .collect(Collectors.toList());
        }
        else
        {
            return new ArrayList<>();
        }
    }

    private PartitionKeyFilter buildFilter(List<String> keys)
    {
        AbstractMap.SimpleEntry<ByteBuffer, BigInteger> filterKey = dataLayer.bridge().getPartitionKey(dataLayer.cqlTable(), dataLayer.partitioner(), keys);
        return PartitionKeyFilter.create(filterKey.getKey(), filterKey.getValue());
    }

    @Override
    public Statistics estimateStatistics()
    {
        OptionalLong estimatedSizeInBytes = dataLayer.calculateTotalSSTableSize();
        return new CassandraSourceStatistics(
        estimatedSizeInBytes,
        OptionalLong.empty(), // numRows - not calculated for now
        Collections.emptyMap() // columnStats - empty for now
        );
    }

    @Override
    public CustomMetric[] supportedCustomMetrics()
    {
        return new CustomMetric[] {
        new TotalSummaryReadDuration(),
        new TotalOpenedSSTableDuration(),
        new TotalCorruptSSTableCount(),
        new TotalSkippedSSTableCount(),
        new TotalS3HeadObjectDuration(),
        new TotalS3GetObjectDuration(),
        new TotalMutableMetadataDriftCount(),
        new TotalMutableMetadataHeadFallbackCount()
        };
    }
}
