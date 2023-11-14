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

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.CassandraDataSourceHelper;
import org.apache.cassandra.spark.data.ClientConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.partitioning.Distribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * A concrete implementation for the {@link CassandraDataSource}
 */
public class CassandraDataSource implements DataSourceV2, ReadSupport, DataSourceRegister
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataSource.class);
    private DataLayer dataLayer;

    public CassandraDataSource()
    {
        CassandraBridgeFactory.validateBridges();
    }

    @Override
    public String shortName()
    {
        return "cassandraBulkRead";
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options)
    {
        if (dataLayer == null)
        {
            dataLayer = getDataLayer(options);
        }
        return new SSTableSourceReader(dataLayer);
    }

    public DataLayer getDataLayer(DataSourceOptions options)
    {
        return CassandraDataSourceHelper.getDataLayer(options.asMap(), this::initializeDataLayer);
    }

    @VisibleForTesting
    void initializeDataLayer(CassandraDataLayer dataLayer, ClientConfig config)
    {
        dataLayer.initialize(config);
    }

    public static class SSTableSourceReader
            implements DataSourceReader, Serializable, SupportsPushDownFilters, SupportsPushDownRequiredColumns, Partitioning
    {
        private static final long serialVersionUID = -6622216100571485739L;
        private Filter[] pushedFilters = new Filter[0];
        private final DataLayer dataLayer;
        private StructType requiredSchema = null;

        SSTableSourceReader(@NotNull DataLayer dataLayer)
        {
            this.dataLayer = dataLayer;
        }

        @Override
        public StructType readSchema()
        {
            return dataLayer.structType();
        }

        @Override
        public List<InputPartition<InternalRow>> planInputPartitions()
        {
            List<PartitionKeyFilter> partitionKeyFilters = new ArrayList<>();

            List<String> partitionKeyColumnNames = dataLayer.cqlTable().partitionKeys().stream()
                                                                                       .map(CqlField::name)
                                                                                       .collect(Collectors.toList());
            Map<String, List<String>> partitionKeyValues =
                    FilterUtils.extractPartitionKeyValues(pushedFilters, new HashSet<>(partitionKeyColumnNames));
            if (partitionKeyValues.size() > 0)
            {
                List<List<String>> orderedValues = partitionKeyColumnNames.stream()
                                                                          .map(partitionKeyValues::get)
                                                                          .collect(Collectors.toList());
                FilterUtils.cartesianProduct(orderedValues).forEach(keys -> {
                    AbstractMap.SimpleEntry<ByteBuffer, BigInteger> filterKey =
                            dataLayer.bridge().getPartitionKey(dataLayer.cqlTable(), dataLayer.partitioner(), keys);
                    partitionKeyFilters.add(PartitionKeyFilter.create(filterKey.getKey(), filterKey.getValue()));
                });
            }
            LOGGER.info("Creating data reader factories numPartitions={}", dataLayer.partitionCount());
            return IntStream.range(0, dataLayer.partitionCount())
                            .mapToObj(partitionId -> new SerializableInputPartition(partitionId, dataLayer, requiredSchema, partitionKeyFilters))
                            .collect(Collectors.toList());
        }

        public static class SerializableInputPartition implements InputPartition<InternalRow>
        {
            private static final long serialVersionUID = -7916492108742137769L;
            private final int partitionId;
            @NotNull
            private final DataLayer dataLayer;
            @NotNull
            private final StructType requiredSchema;
            @NotNull
            private final List<PartitionKeyFilter> partitionKeyFilters;

            public SerializableInputPartition(int partitionId,
                                              @NotNull DataLayer dataLayer,
                                              @NotNull StructType requiredSchema,
                                              @NotNull List<PartitionKeyFilter> partitionKeyFilters)
            {
                this.partitionId = partitionId;
                this.dataLayer = dataLayer;
                this.requiredSchema = requiredSchema;
                this.partitionKeyFilters = partitionKeyFilters;
            }

            @Override
            @NotNull
            public InputPartitionReader<InternalRow> createPartitionReader()
            {
                return new SparkRowIterator(partitionId, dataLayer, requiredSchema, partitionKeyFilters);
            }
        }

        /**
         * Pushes down filters, and returns filters that need to be evaluated after scanning
         *
         * @param filters the filters in the query
         * @return filters that need to be evaluated after scanning
         */
        @Override
        public Filter[] pushFilters(Filter[] filters)
        {
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(filters);

            List<Filter> supportedFilters = Lists.newArrayList(filters);
            supportedFilters.removeAll(Arrays.asList(unsupportedFilters));
            pushedFilters = supportedFilters.stream().toArray(Filter[]::new);

            return unsupportedFilters;
        }

        @Override
        public Filter[] pushedFilters()
        {
            return pushedFilters;
        }

        @Override
        public void pruneColumns(StructType requiredSchema)
        {
            this.requiredSchema = requiredSchema;
        }

        @Override
        public int numPartitions()
        {
            return dataLayer.partitionCount();
        }

        @Override
        public boolean satisfy(Distribution distribution)
        {
            return true;
        }
    }
}
