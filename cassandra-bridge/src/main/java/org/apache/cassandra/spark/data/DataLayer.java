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

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.DoNothingWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.EmptyStreamScanner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({ "unused", "WeakerAccess" })
public abstract class DataLayer implements Serializable
{
    public static final long serialVersionUID = 42L;

    public DataLayer()
    {
    }

    /**
     * @return SparkSQL table schema expected for reading Partition sizes with PartitionSizeTableProvider.
     */
    public StructType partitionSizeStructType()
    {
        StructType structType = new StructType();
        for (final CqlField field : cqlTable().partitionKeys())
        {
            final MetadataBuilder metadata = fieldMetaData(field);
            structType = structType.add(field.name(),
                                        field.type().sparkSqlType(bigNumberConfig(field)),
                                        true,
                                        metadata.build());
        }

        structType = structType.add("uncompressed", DataTypes.LongType);
        structType = structType.add("compressed", DataTypes.LongType);

        return structType;
    }

    /**
     * Map Cassandra CQL table schema to SparkSQL StructType
     *
     * @return StructType representation of CQL table
     */
    public StructType structType()
    {
        StructType structType = new StructType();
        for (CqlField field : cqlTable().fields())
        {
            // Pass Cassandra field metadata in StructField metadata
            MetadataBuilder metadata = fieldMetaData(field);
            structType = structType.add(field.name(),
                                        field.type().sparkSqlType(bigNumberConfig(field)),
                                        true,
                                        metadata.build());
        }

        // Append the requested feature fields
        for (SchemaFeature feature : requestedFeatures())
        {
            feature.generateDataType(cqlTable(), structType);
            structType = structType.add(feature.field());
        }

        return structType;
    }

    private MetadataBuilder fieldMetaData(final CqlField field)
    {
        final MetadataBuilder metadata = new MetadataBuilder();
        metadata.putLong("position", field.position());
        metadata.putString("cqlType", field.cqlTypeName());
        metadata.putBoolean("isPartitionKey", field.isPartitionKey());
        metadata.putBoolean("isPrimaryKey", field.isPrimaryKey());
        metadata.putBoolean("isClusteringKey", field.isClusteringColumn());
        metadata.putBoolean("isStaticColumn", field.isStaticColumn());
        metadata.putBoolean("isValueColumn", field.isValueColumn());
        return metadata;
    }

    public List<SchemaFeature> requestedFeatures()
    {
        return Collections.emptyList();
    }

    /**
     * DataLayer can override this method to return the BigInteger/BigDecimal precision/scale values for a given column
     *
     * @param field the CQL field
     * @return a BigNumberConfig object that specifies the desired precision/scale for BigDecimal and BigInteger
     */
    public BigNumberConfig bigNumberConfig(CqlField field)
    {
        return BigNumberConfig.DEFAULT;
    }

    /**
     * @return Cassandra version (3.0, 4.0 etc)
     */
    public CassandraVersion version()
    {
        return bridge().getVersion();
    }

    /**
     * @return version-specific CassandraBridge wrapping shaded packages
     */
    public abstract CassandraBridge bridge();

    public abstract int partitionCount();

    /**
     * @return CqlTable object for table being read, batch/bulk read jobs only
     */
    public abstract CqlTable cqlTable();

    public abstract boolean isInPartition(int partitionId, BigInteger token, ByteBuffer key);

    public List<PartitionKeyFilter> partitionKeyFiltersInRange(
            int partitionId,
            List<PartitionKeyFilter> partitionKeyFilters) throws NoMatchFoundException
    {
        return partitionKeyFilters;
    }

    public abstract CommitLogProvider commitLogs(int partitionId);

    public abstract TableIdLookup tableIdLookup();

    /**
     * DataLayer implementation should provide a SparkRangeFilter to filter out partitions and mutations
     * that do not overlap with the Spark worker's token range
     *
     * @param partitionId the partitionId for the task
     * @return SparkRangeFilter for the Spark worker's token range
     */
    public SparkRangeFilter sparkRangeFilter(int partitionId)
    {
        return null;
    }

    /**
     * DataLayer implementation should provide an ExecutorService for doing blocking I/O
     * when opening SSTable readers or reading CDC CommitLogs.
     * It is the responsibility of the DataLayer implementation to appropriately size and manage this ExecutorService.
     *
     * @return executor service
     */
    protected abstract ExecutorService executorService();

    /**
     * @param partitionId         the partitionId of the task
     * @param sparkRangeFilter    spark range filter
     * @param partitionKeyFilters the list of partition key filters
     * @return set of SSTables
     */
    public abstract SSTablesSupplier sstables(int partitionId,
                                              @Nullable SparkRangeFilter sparkRangeFilter,
                                              @NotNull List<PartitionKeyFilter> partitionKeyFilters);

    public abstract Partitioner partitioner();

    /**
     * It specifies the minimum number of replicas required for CDC.
     * For example, the minimum number of PartitionUpdates for compaction,
     * and the minimum number of replicas to pull logs from to proceed to compaction.
     *
     * @return the minimum number of replicas. The returned value must be 1 or more.
     */
    public int minimumReplicasForCdc()
    {
        return 1;
    }

    /**
     * @return a string that uniquely identifies this Spark job
     */
    public abstract String jobId();

    /**
     * Override this method with a Watermarker implementation
     * that persists high and low watermarks per Spark partition between Streaming batches
     *
     * @return watermarker for persisting high and low watermark and late updates
     */
    public Watermarker cdcWatermarker()
    {
        return DoNothingWatermarker.INSTANCE;
    }

    public Duration cdcWatermarkWindow()
    {
        return Duration.ofSeconds(30);
    }

    public StreamScanner openCdcScanner(int partitionId, @Nullable CdcOffsetFilter offset)
    {
        return bridge().getCdcScanner(partitionId,
                                      cqlTable(),
                                      partitioner(),
                                      commitLogs(partitionId),
                                      tableIdLookup(),
                                      stats(),
                                      sparkRangeFilter(partitionId),
                                      offset,
                                      minimumReplicasForCdc(),
                                      cdcWatermarker(),
                                      jobId(),
                                      executorService(),
                                      timeProvider());
    }

    public StreamScanner openCompactionScanner(int partitionId, List<PartitionKeyFilter> partitionKeyFilters)
    {
        return openCompactionScanner(partitionId, partitionKeyFilters, null);
    }

    /**
     * When true the SSTableReader should attempt to find the offset into the Data.db file for the Spark worker's
     * token range. This works by first binary searching the Summary.db file to find offset into Index.db file,
     * then reading the Index.db from the Summary.db offset to find the first offset in the Data.db file
     * that overlaps with the Spark worker's token range. This enables the reader to start reading from the first
     * in-range partition in the Data.db file, and close after reading the last partition. This feature improves
     * scalability as more Spark workers shard the token range into smaller subranges. This avoids wastefully reading
     * the Data.db file for out-of-range partitions.
     *
     * @return true if, the SSTableReader should attempt to read Summary.db and Index.db files
     *         to find the start index offset into the Data.db file that overlaps with the Spark workers token range
     */
    public boolean readIndexOffset()
    {
        return true;
    }

    /**
     * When true the SSTableReader should only read repaired SSTables from a single 'primary repair' replica
     * and read unrepaired SSTables at the user set consistency level
     *
     * @return true if the SSTableReader should only read repaired SSTables on single 'repair primary' replica
     */
    public boolean useIncrementalRepair()
    {
        return true;
    }

    /**
     * @return CompactionScanner for iterating over one or more SSTables, compacting data and purging tombstones
     */
    public StreamScanner<Rid> openCompactionScanner(int partitionId,
                                                    List<PartitionKeyFilter> partitionKeyFilters,
                                                    @Nullable PruneColumnFilter columnFilter)
    {
        List<PartitionKeyFilter> filtersInRange;
        try
        {
            filtersInRange = partitionKeyFiltersInRange(partitionId, partitionKeyFilters);
        }
        catch (NoMatchFoundException exception)
        {
            return EmptyStreamScanner.INSTANCE;
        }
        SparkRangeFilter sparkRangeFilter = sparkRangeFilter(partitionId);
        return bridge().getCompactionScanner(cqlTable(),
                                             partitioner(),
                                             sstables(partitionId, sparkRangeFilter, filtersInRange),
                                             sparkRangeFilter,
                                             filtersInRange,
                                             columnFilter,
                                             timeProvider(),
                                             readIndexOffset(),
                                             useIncrementalRepair(),
                                             stats());
    }

    /**
     * @param partitionId Spark partition id
     * @return a PartitionSizeIterator that iterates over Index.db files to calculate partition size.
     */
    public StreamScanner<IndexEntry> openPartitionSizeIterator(final int partitionId)
    {
        final SparkRangeFilter rangeFilter = sparkRangeFilter(partitionId);
        return bridge().getPartitionSizeIterator(cqlTable(), partitioner(), sstables(partitionId, rangeFilter, List.of()),
                                                 rangeFilter, timeProvider(), stats(), executorService());
    }

    /**
     * @return a TimeProvider that returns the time now in seconds. User can override with their own provider
     */
    public TimeProvider timeProvider()
    {
        return bridge().timeProvider();
    }

    /**
     * @param filters array of push down filters that
     * @return an array of push filters that are <b>not</b> supported by this data layer
     */
    public Filter[] unsupportedPushDownFilters(Filter[] filters)
    {
        Set<String> partitionKeys = cqlTable().partitionKeys().stream()
                                              .map(key -> StringUtils.lowerCase(key.name()))
                                              .collect(Collectors.toSet());

        List<Filter> unsupportedFilters = new ArrayList<>(filters.length);
        for (Filter filter : filters)
        {
            if (filter instanceof EqualTo || filter instanceof In)
            {
                String columnName = StringUtils.lowerCase(filter instanceof EqualTo
                        ? ((EqualTo) filter).attribute()
                        : ((In) filter).attribute());

                if (partitionKeys.contains(columnName))
                {
                    partitionKeys.remove(columnName);
                }
                else
                {
                    // Only partition keys are supported
                    unsupportedFilters.add(filter);
                }
            }
            else
            {
                // Push down filters other than EqualTo & In not supported yet
                unsupportedFilters.add(filter);
            }
        }
        // If the partition keys are not in the filter, we disable push down
        return partitionKeys.size() > 0 ? filters : unsupportedFilters.toArray(new Filter[0]);
    }

    /**
     * Override to plug in your own Stats instrumentation for recording internal events
     *
     * @return Stats implementation to record internal events
     */
    public Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }
}
