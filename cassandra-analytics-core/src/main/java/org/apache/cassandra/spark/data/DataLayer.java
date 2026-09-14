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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.EmptyStreamScanner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;
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
        for (CqlField field : cqlTable().partitionKeys())
        {
            MetadataBuilder metadata = fieldMetaData(field);
            structType = structType.add(field.name(),
                                        typeConverter().sparkSqlType(field, bigNumberConfig(field)),
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
                                        typeConverter().sparkSqlType(field, bigNumberConfig(field)),
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

    private MetadataBuilder fieldMetaData(CqlField field)
    {
        MetadataBuilder metadata = new MetadataBuilder();
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

    /**
     * @return SparkSQL type converter that maps version-specific Cassandra types to SparkSQL types
     */
    public SparkSqlTypeConverter typeConverter()
    {
        return CassandraBridgeFactory.getSparkSql(version());
    }

    public abstract int partitionCount();

    /**
     * @return CqlTable object for table being read, batch/bulk read jobs only
     */
    public abstract CqlTable cqlTable();

    public abstract boolean isInPartition(int partitionId, BigInteger token, ByteBuffer key);

    /**
     * @return a TimeProvider
     */
    public abstract TimeProvider timeProvider();

    public List<PartitionKeyFilter> partitionKeyFiltersInRange(
    int partitionId,
    List<PartitionKeyFilter> partitionKeyFilters) throws NoMatchFoundException
    {
        return partitionKeyFilters;
    }

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
     * Returns {@link SSTableTimeRangeFilter} to filter out SSTables based on min and max timestamp.
     *
     * @return {@link SSTableTimeRangeFilter}
     */
    @NotNull
    public SSTableTimeRangeFilter sstableTimeRangeFilter()
    {
        return SSTableTimeRangeFilter.ALL;
    }

    /**
     * DataLayer implementation should provide an ExecutorService for doing blocking I/O
     * when opening SSTable readers.
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
     * @return a string that uniquely identifies this Spark job
     */
    public abstract String jobId();

    public StreamScanner openCompactionScanner(int partitionId, List<PartitionKeyFilter> partitionKeyFilters, SSTableTimeRangeFilter sstableTimeRangeFilter)
    {
        return openCompactionScanner(partitionId, partitionKeyFilters, sstableTimeRangeFilter, null, Collections.emptyList());
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
     * to find the start index offset into the Data.db file that overlaps with the Spark workers token range
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

    public boolean saiFilteringEnabled()
    {
        return ClientConfig.DEFAULT_SAI_FILTERING_ENABLED;
    }

    public int saiMaxCandidateTokens()
    {
        return ClientConfig.DEFAULT_SAI_MAX_CANDIDATE_TOKENS;
    }

    /**
     * @return CompactionScanner for iterating over one or more SSTables, compacting data and purging tombstones
     */
    public StreamScanner<RowData> openCompactionScanner(int partitionId,
                                                        List<PartitionKeyFilter> partitionKeyFilters,
                                                        SSTableTimeRangeFilter sstableTimeRangeFilter,
                                                        @Nullable PruneColumnFilter columnFilter,
                                                        @NotNull List<SaiFilter> saiFilters)
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
                                             sstableTimeRangeFilter,
                                             columnFilter,
                                             timeProvider(),
                                             readIndexOffset(),
                                             useIncrementalRepair(),
                                             stats(),
                                             saiFilteringEnabled() ? saiFilters : Collections.emptyList(),
                                             saiMaxCandidateTokens());
    }

    /**
     * @param partitionId Spark partition id
     * @return a PartitionSizeIterator that iterates over Index.db files to calculate partition size.
     */
    public StreamScanner<IndexEntry> openPartitionSizeIterator(int partitionId)
    {
        SparkRangeFilter rangeFilter = sparkRangeFilter(partitionId);
        return bridge().getPartitionSizeIterator(cqlTable(), partitioner(), sstables(partitionId, rangeFilter, Collections.emptyList()),
                                                 rangeFilter, timeProvider(), stats(), executorService());
    }

    /**
     * @param filters array of push down filters that
     * @return an array of push filters that are <b>not</b> supported by this data layer
     */
    public Filter[] unsupportedPushDownFilters(Filter[] filters)
    {
        Set<String> partitionKeys = cqlTable().partitionKeys().stream()
                                              .map(key -> lowerCase(key.name()))
                                              .collect(Collectors.toSet());

        List<Filter> unsupportedFilters = new ArrayList<>(filters.length);
        for (Filter filter : filters)
        {
            if (filter instanceof EqualTo || filter instanceof In)
            {
                String columnName = lowerCase(filter instanceof EqualTo
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
     * @return SAI definitions available for this table.
     */
    @NotNull
    public List<SaiIndex> saiIndexes()
    {
        return Collections.emptyList();
    }

    /**
     * Extracts Spark predicates that can safely be used as SAI pruning hints.
     *
     * These predicates remain in {@link #unsupportedPushDownFilters(Filter[])} so Spark evaluates them after the
     * Cassandra reader has reconciled all candidate partitions. SAI therefore narrows physical I/O without becoming
     * part of the correctness boundary.
     */
    @NotNull
    public List<SaiFilter> saiFilters(@NotNull Filter[] filters)
    {
        if (saiIndexes().isEmpty())
        {
            return Collections.emptyList();
        }

        List<SaiFilter> result = new ArrayList<>();
        for (Filter filter : filters)
        {
            addSaiFilters(filter, result);
        }
        return result;
    }

    /**
     * Adds SAI-capable predicates from a conjunctive Spark filter tree.
     *
     * A partially-indexable AND can still use its indexable branch because every final match must satisfy that
     * branch. OR is not supported by Cassandra 5.0 SAI, so an OR subtree is never used for SAI pruning.
     */
    private void addSaiFilters(@NotNull Filter filter, @NotNull List<SaiFilter> result)
    {
        if (filter instanceof And)
        {
            And and = (And) filter;
            addSaiFilters(and.left(), result);
            addSaiFilters(and.right(), result);
            return;
        }
        if (filter instanceof Or)
        {
            return;
        }

        SaiFilter saiFilter = toSaiFilter(filter);
        if (saiFilter != null)
        {
            result.add(saiFilter);
        }
    }

    @Nullable
    private SaiFilter toSaiFilter(@NotNull Filter filter)
    {
        String attribute = filterAttribute(filter);
        Object value = filterValue(filter);
        SaiFilter.Operator operator = saiOperator(filter);
        if (attribute == null || value == null || operator == null)
        {
            return null;
        }

        SaiIndex index = saiIndexForAttribute(attribute);
        return index == null ? null : new SaiFilter(index, operator, String.valueOf(value));
    }

    /**
     * Resolves a Spark attribute to an indexed Cassandra column.
     *
     * Exact spelling always wins. Case-insensitive matching is retained for the normal unquoted identifier path,
     * but only when all matching indexes refer to the same Cassandra column spelling. If, for example, both
     * {@code Foo} and {@code foo} are indexed, an attribute such as {@code FOO} is deliberately not used for SAI
     * pruning because choosing either index could create false negatives.
     */
    @Nullable
    private SaiIndex saiIndexForAttribute(@NotNull String attribute)
    {
        CqlField field = resolveField(attribute);
        if (field == null)
        {
            return null;
        }

        SaiIndex match = null;
        for (SaiIndex index : saiIndexes())
        {
            if (index.column().equals(field.name()))
            {
                if (match != null)
                {
                    return null;
                }
                match = index;
            }
        }
        return match;
    }

    @Nullable
    private CqlField resolveField(@NotNull String attribute)
    {
        // exact match always wins
        for (CqlField field : cqlTable().fields())
        {
            if (field.name().equals(attribute))
            {
                return field;
            }
        }

        // otherwise case-insensitive resolution is safe only if
        // there is exactly one matching table column
        CqlField match = null;
        for (CqlField field : cqlTable().fields())
        {
            if (field.name().equalsIgnoreCase(attribute))
            {
                if (match != null)
                {
                    return null;
                }
                match = field;
            }
        }
        return match;
    }

    @Nullable
    private static String filterAttribute(Filter filter)
    {
        if (filter instanceof EqualTo)
        {
            return ((EqualTo) filter).attribute();
        }
        if (filter instanceof GreaterThan)
        {
            return ((GreaterThan) filter).attribute();
        }
        if (filter instanceof GreaterThanOrEqual)
        {
            return ((GreaterThanOrEqual) filter).attribute();
        }
        if (filter instanceof LessThan)
        {
            return ((LessThan) filter).attribute();
        }
        if (filter instanceof LessThanOrEqual)
        {
            return ((LessThanOrEqual) filter).attribute();
        }
        return null;
    }

    @Nullable
    private static Object filterValue(Filter filter)
    {
        if (filter instanceof EqualTo)
        {
            return ((EqualTo) filter).value();
        }
        if (filter instanceof GreaterThan)
        {
            return ((GreaterThan) filter).value();
        }
        if (filter instanceof GreaterThanOrEqual)
        {
            return ((GreaterThanOrEqual) filter).value();
        }
        if (filter instanceof LessThan)
        {
            return ((LessThan) filter).value();
        }
        if (filter instanceof LessThanOrEqual)
        {
            return ((LessThanOrEqual) filter).value();
        }
        return null;
    }

    @Nullable
    private static SaiFilter.Operator saiOperator(Filter filter)
    {
        if (filter instanceof EqualTo)
        {
            return SaiFilter.Operator.EQ;
        }
        if (filter instanceof GreaterThan)
        {
            return SaiFilter.Operator.GT;
        }
        if (filter instanceof GreaterThanOrEqual)
        {
            return SaiFilter.Operator.GTE;
        }
        if (filter instanceof LessThan)
        {
            return SaiFilter.Operator.LT;
        }
        if (filter instanceof LessThanOrEqual)
        {
            return SaiFilter.Operator.LTE;
        }
        return null;
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

    static String lowerCase(String s)
    {
        if (s == null)
        {
            return s;
        }
        return s.toLowerCase();
    }
}
