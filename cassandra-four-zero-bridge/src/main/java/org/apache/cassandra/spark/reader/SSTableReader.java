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

package org.apache.cassandra.spark.reader;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.analytics.reader.common.RawInputStream;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public class SSTableReader implements SparkSSTableReader, Scannable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableReader.class);

    private final TableMetadata metadata;
    @NotNull
    private final SSTable ssTable;
    private final StatsMetadata statsMetadata;
    @NotNull
    private final Version version;
    @NotNull
    private final DecoratedKey first;
    @NotNull
    private final DecoratedKey last;
    @NotNull
    private final BigInteger firstToken;
    @NotNull
    private final BigInteger lastToken;
    private final SerializationHeader header;
    private final DeserializationHelper helper;
    @NotNull
    private final AtomicReference<SSTableStreamReader> reader = new AtomicReference<>(null);
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    @NotNull
    private final List<PartitionKeyFilter> partitionKeyFilters;
    @NotNull
    private final Stats stats;
    @Nullable
    private Long startOffset = null;
    private Long openedNanos = null;
    @NotNull
    private final Function<StatsMetadata, Boolean> isRepaired;

    public static class Builder
    {
        @NotNull
        final TableMetadata metadata;
        @NotNull
        final SSTable ssTable;
        @Nullable
        PruneColumnFilter columnFilter = null;
        boolean readIndexOffset = true;
        @NotNull
        Stats stats = Stats.DoNothingStats.INSTANCE;
        boolean useIncrementalRepair = true;
        boolean isRepairPrimary = false;
        Function<StatsMetadata, Boolean> isRepaired = stats -> stats.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
        @Nullable
        SparkRangeFilter sparkRangeFilter = null;
        @NotNull
        final List<PartitionKeyFilter> partitionKeyFilters = new ArrayList<>();

        Builder(@NotNull TableMetadata metadata, @NotNull SSTable ssTable)
        {
            this.metadata = metadata;
            this.ssTable = ssTable;
        }

        public Builder withSparkRangeFilter(@Nullable SparkRangeFilter sparkRangeFilter)
        {
            this.sparkRangeFilter = sparkRangeFilter;
            return this;
        }

        public Builder withPartitionKeyFilters(@NotNull Collection<PartitionKeyFilter> partitionKeyFilters)
        {
            this.partitionKeyFilters.addAll(partitionKeyFilters);
            return this;
        }

        public Builder withPartitionKeyFilter(@NotNull PartitionKeyFilter partitionKeyFilter)
        {
            partitionKeyFilters.add(partitionKeyFilter);
            return this;
        }

        public Builder withColumnFilter(@Nullable PruneColumnFilter columnFilter)
        {
            this.columnFilter = columnFilter;
            return this;
        }

        public Builder withReadIndexOffset(boolean readIndexOffset)
        {
            this.readIndexOffset = readIndexOffset;
            return this;
        }

        public Builder withStats(@NotNull Stats stats)
        {
            this.stats = stats;
            return this;
        }

        public Builder useIncrementalRepair(boolean useIncrementalRepair)
        {
            this.useIncrementalRepair = useIncrementalRepair;
            return this;
        }

        public Builder isRepairPrimary(boolean isRepairPrimary)
        {
            this.isRepairPrimary = isRepairPrimary;
            return this;
        }

        public Builder withIsRepairedFunction(Function<StatsMetadata, Boolean> isRepaired)
        {
            this.isRepaired = isRepaired;
            return this;
        }

        public SSTableReader build() throws IOException
        {
            return new SSTableReader(metadata,
                                     ssTable,
                                     sparkRangeFilter,
                                     partitionKeyFilters,
                                     columnFilter,
                                     readIndexOffset,
                                     stats,
                                     useIncrementalRepair,
                                     isRepairPrimary,
                                     isRepaired);
        }
    }

    public static Builder builder(@NotNull TableMetadata metadata, @NotNull SSTable ssTable)
    {
        return new Builder(metadata, ssTable);
    }

    // CHECKSTYLE IGNORE: Constructor with many parameters
    public SSTableReader(@NotNull TableMetadata metadata,
                         @NotNull SSTable ssTable,
                         @Nullable SparkRangeFilter sparkRangeFilter,
                         @NotNull List<PartitionKeyFilter> partitionKeyFilters,
                         @Nullable PruneColumnFilter columnFilter,
                         boolean readIndexOffset,
                         @NotNull Stats stats,
                         boolean useIncrementalRepair,
                         boolean isRepairPrimary,
                         @NotNull Function<StatsMetadata, Boolean> isRepaired) throws IOException
    {
        long startTimeNanos = System.nanoTime();
        long now;
        this.ssTable = ssTable;
        this.stats = stats;
        this.isRepaired = isRepaired;
        this.sparkRangeFilter = sparkRangeFilter;

        File file = constructFilename(metadata.keyspace, metadata.name, ssTable.getDataFileName());
        Descriptor descriptor = Descriptor.fromFilename(file);
        this.version = descriptor.version;

        SummaryDbUtils.Summary summary = null;
        Pair<DecoratedKey, DecoratedKey> keys = Pair.create(null, null);
        try
        {
            now = System.nanoTime();
            summary = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable);
            stats.readSummaryDb(ssTable, System.nanoTime() - now);
            keys = Pair.create(summary.first(), summary.last());
        }
        catch (IOException exception)
        {
            LOGGER.warn("Failed to read Summary.db file ssTable='{}'", ssTable, exception);
        }

        if (keys.left == null || keys.right == null)
        {
            LOGGER.warn("Could not load first and last key from Summary.db file, so attempting Index.db fileName={}",
                        ssTable.getDataFileName());
            now = System.nanoTime();
            keys = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable);
            stats.readIndexDb(ssTable, System.nanoTime() - now);
        }

        if (keys.left == null || keys.right == null)
        {
            throw new IOException("Could not load SSTable first or last tokens");
        }

        this.first = keys.left;
        this.last = keys.right;
        this.firstToken = ReaderUtils.tokenToBigInteger(first.getToken());
        this.lastToken = ReaderUtils.tokenToBigInteger(last.getToken());
        TokenRange readerRange = range();

        List<PartitionKeyFilter> matchingKeyFilters = partitionKeyFilters.stream()
                .filter(filter -> readerRange.contains(filter.token()))
                .collect(Collectors.toList());
        boolean overlapsSparkRange = sparkRangeFilter == null || SparkSSTableReader.overlaps(this, sparkRangeFilter.tokenRange());
        if (!overlapsSparkRange  // SSTable doesn't overlap with Spark worker token range
                || (matchingKeyFilters.isEmpty() && !partitionKeyFilters.isEmpty()))  // No matching partition key filters overlap with SSTable
        {
            this.partitionKeyFilters = Collections.emptyList();
            stats.skippedSSTable(sparkRangeFilter, partitionKeyFilters, firstToken, lastToken);
            LOGGER.info("Ignoring SSTableReader with firstToken={} lastToken={}, does not overlap with any filter",
                        firstToken, lastToken);
            statsMetadata = null;
            header = null;
            helper = null;
            this.metadata = null;
            return;
        }

        if (!matchingKeyFilters.isEmpty())
        {
            List<PartitionKeyFilter> matchInBloomFilter =
                    ReaderUtils.filterKeyInBloomFilter(ssTable, metadata.partitioner, descriptor, matchingKeyFilters);
            this.partitionKeyFilters = ImmutableList.copyOf(matchInBloomFilter);

            // Check if required keys are actually present
            if (matchInBloomFilter.isEmpty() || !ReaderUtils.anyFilterKeyInIndex(ssTable, matchInBloomFilter))
            {
                if (matchInBloomFilter.isEmpty())
                {
                    stats.missingInBloomFilter();
                }
                else
                {
                    stats.missingInIndex();
                }
                LOGGER.info("Ignoring SSTable {}, no match found in index file for key filters",
                            this.ssTable.getDataFileName());
                statsMetadata = null;
                header = null;
                helper = null;
                this.metadata = null;
                return;
            }
        }
        else
        {
            this.partitionKeyFilters = ImmutableList.copyOf(partitionKeyFilters);
        }

        Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable, descriptor);

        ValidationMetadata validation = (ValidationMetadata) componentMap.get(MetadataType.VALIDATION);
        if (validation != null && !validation.partitioner.equals(metadata.partitioner.getClass().getName()))
        {
            throw new IllegalStateException("Partitioner in ValidationMetadata does not match TableMetaData: "
                                          + validation.partitioner + " vs. " + metadata.partitioner.getClass().getName());
        }

        this.statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);
        SerializationHeader.Component headerComp = (SerializationHeader.Component) componentMap.get(MetadataType.HEADER);
        if (headerComp == null)
        {
            throw new IOException("Cannot read SSTable if cannot deserialize stats header info");
        }

        if (useIncrementalRepair && !isRepairPrimary && isRepaired())
        {
            stats.skippedRepairedSSTable(ssTable, statsMetadata.repairedAt);
            LOGGER.info("Ignoring repaired SSTable on non-primary repair replica ssTable='{}' repairedAt={}",
                        ssTable, statsMetadata.repairedAt);
            header = null;
            helper = null;
            this.metadata = null;
            return;
        }

        Set<String> columnNames = Streams.concat(metadata.columns().stream(),
                                                 metadata.staticColumns().stream())
                                         .map(column -> column.name.toString())
                                         .collect(Collectors.toSet());
        Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        droppedColumns.putAll(buildDroppedColumns(metadata.keyspace,
                                                  metadata.name,
                                                  ssTable,
                                                  headerComp.getRegularColumns(),
                                                  columnNames,
                                                  ColumnMetadata.Kind.REGULAR));
        droppedColumns.putAll(buildDroppedColumns(metadata.keyspace,
                                                  metadata.name,
                                                  ssTable,
                                                  headerComp.getStaticColumns(),
                                                  columnNames,
                                                  ColumnMetadata.Kind.STATIC));
        if (!droppedColumns.isEmpty())
        {
            LOGGER.info("Rebuilding table metadata with dropped columns numDroppedColumns={} ssTable='{}'",
                        droppedColumns.size(), ssTable);
            metadata = metadata.unbuild().droppedColumns(droppedColumns).build();
        }

        this.header = headerComp.toHeader(metadata);
        this.helper = new DeserializationHelper(metadata,
                                                MessagingService.VERSION_30,
                                                DeserializationHelper.Flag.FROM_REMOTE,
                                                buildColumnFilter(metadata, columnFilter));
        this.metadata = metadata;

        if (readIndexOffset && summary != null)
        {
            SummaryDbUtils.Summary finalSummary = summary;
            extractRange(sparkRangeFilter, partitionKeyFilters)
                    .ifPresent(range -> readOffsets(finalSummary.summary(), range));
        }
        else
        {
            LOGGER.warn("Reading SSTable without looking up start/end offset, performance will potentially be degraded");
        }

        // Open SSTableStreamReader so opened in parallel inside thread pool
        // and buffered + ready to go when CompactionIterator starts reading
        reader.set(new SSTableStreamReader());
        stats.openedSSTable(ssTable, System.nanoTime() - startTimeNanos);
        this.openedNanos = System.nanoTime();
    }

    /**
     * Constructs full file path for a given combination of keyspace, table, and data file name,
     * while adjusting for data files with non-standard names prefixed with keyspace and table
     *
     * @param keyspace Name of the keyspace
     * @param table    Name of the table
     * @param filename Name of the data file
     * @return A full file path, adjusted for non-standard file names
     */
    @VisibleForTesting
    @NotNull
    static File constructFilename(@NotNull String keyspace, @NotNull String table, @NotNull String filename)
    {
        String[] components = filename.split("-");
        if (components.length == 6
                && components[0].equals(keyspace)
                && components[1].equals(table))
        {
            filename = filename.substring(keyspace.length() + table.length() + 2);
        }

        return new File(String.format("./%s/%s", keyspace, table), filename);
    }

    private static Map<ByteBuffer, DroppedColumn> buildDroppedColumns(String keyspace,
                                                                      String table,
                                                                      SSTable ssTable,
                                                                      Map<ByteBuffer, AbstractType<?>> columns,
                                                                      Set<String> columnNames,
                                                                      ColumnMetadata.Kind kind)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
        {
            String colName = UTF8Type.instance.getString((entry.getKey()));
            if (!columnNames.contains(colName))
            {
                AbstractType<?> type = entry.getValue();
                LOGGER.warn("Dropped column found colName={} sstable='{}'", colName, ssTable);
                ColumnMetadata column = new ColumnMetadata(keyspace,
                                                           table,
                                                           ColumnIdentifier.getInterned(colName, true),
                                                           type,
                                                           ColumnMetadata.NO_POSITION,
                                                           kind);
                long droppedTime = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis())
                                 - TimeUnit.MINUTES.toMicros(60);
                droppedColumns.put(entry.getKey(), new DroppedColumn(column, droppedTime));
            }
        }
        return droppedColumns;
    }

    /**
     * Merge all the partition key filters to give the token range we care about.
     * If no partition key filters, then use the Spark worker token range.
     *
     * @param sparkRangeFilter    optional spark range filter
     * @param partitionKeyFilters list of partition key filters
     * @return the token range we care about for this Spark worker
     */
    public static Optional<TokenRange> extractRange(@Nullable SparkRangeFilter sparkRangeFilter,
                                                    @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        Optional<TokenRange> partitionKeyRange = partitionKeyFilters.stream()
                                                                    .map(PartitionKeyFilter::tokenRange)
                                                                    .reduce(TokenRange::merge);
        return partitionKeyRange.isPresent()
                ? partitionKeyRange
                : Optional.ofNullable(sparkRangeFilter != null ? sparkRangeFilter.tokenRange() : null);
    }

    /**
     * Read Data.db offsets by binary searching Summary.db into Index.db, then reading offsets in Index.db
     *
     * @param indexSummary Summary.db index summary
     * @param range        token range we care about for this Spark worker
     */
    private void readOffsets(IndexSummary indexSummary, TokenRange range)
    {
        try
        {
            // If start is null we failed to find an overlapping token in the Index.db file,
            // this is unlikely as we already pre-filter the SSTable based on the start-end token range.
            // But in this situation we read the entire Data.db file to be safe, even if it hits performance.
            startOffset = IndexDbUtils.findDataDbOffset(indexSummary, range, metadata.partitioner, ssTable, stats);
            if (startOffset == null)
            {
                LOGGER.error("Failed to find Data.db start offset, performance will be degraded sstable='{}'", ssTable);
            }
        }
        catch (IOException exception)
        {
            LOGGER.warn("IOException finding SSTable offsets, cannot skip directly to start offset in Data.db. "
                      + "Performance will be degraded.", exception);
        }
    }

    /**
     * Build a ColumnFilter if we need to prune any columns for more efficient deserialization of the SSTable
     *
     * @param metadata     TableMetadata object
     * @param columnFilter prune column filter
     * @return ColumnFilter if and only if we can prune any columns when deserializing the SSTable,
     *                      otherwise return null
     */
    @Nullable
    private static ColumnFilter buildColumnFilter(TableMetadata metadata, @Nullable PruneColumnFilter columnFilter)
    {
        if (columnFilter == null)
        {
            return null;
        }
        List<ColumnMetadata> include = metadata.columns().stream()
                .filter(column -> columnFilter.includeColumn(column.name.toString()))
                .collect(Collectors.toList());
        if (include.size() == metadata.columns().size())
        {
            return null;  // No columns pruned
        }
        return ColumnFilter.allRegularColumnsBuilder(metadata, false)
                           .addAll(include)
                           .build();
    }

    public SSTable sstable()
    {
        return ssTable;
    }

    public boolean ignore()
    {
        return reader.get() == null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata.keyspace, metadata.name, ssTable);
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof SSTableReader
            && this.metadata.keyspace.equals(((SSTableReader) other).metadata.keyspace)
            && this.metadata.name.equals(((SSTableReader) other).metadata.name)
            && this.ssTable.equals(((SSTableReader) other).ssTable);
    }

    public boolean isRepaired()
    {
        return isRepaired.apply(statsMetadata);
    }

    public DecoratedKey first()
    {
        return first;
    }

    public DecoratedKey last()
    {
        return last;
    }

    public long getMinTimestamp()
    {
        return statsMetadata.minTimestamp;
    }

    public long getMaxTimestamp()
    {
        return statsMetadata.maxTimestamp;
    }

    public StatsMetadata getSSTableMetadata()
    {
        return statsMetadata;
    }

    @Override
    public ISSTableScanner scanner()
    {
        ISSTableScanner result = reader.getAndSet(null);
        if (result == null)
        {
            throw new IllegalStateException("SSTableStreamReader cannot be re-used");
        }
        return result;
    }

    @Override
    @NotNull
    public BigInteger firstToken()
    {
        return firstToken;
    }

    @Override
    @NotNull
    public BigInteger lastToken()
    {
        return lastToken;
    }

    public class SSTableStreamReader implements ISSTableScanner
    {
        private final DataInputStream dis;
        private final DataInputPlus in;
        final RawInputStream dataStream;
        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;
        @Nullable
        private final BigInteger lastToken;
        private long lastTimeNanos = System.nanoTime();

        SSTableStreamReader() throws IOException
        {
            lastToken = sparkRangeFilter != null ? sparkRangeFilter.tokenRange().upperEndpoint() : null;
            @Nullable CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(ssTable, version.hasMaxCompressedLength());
            DataInputStream dataInputStream = new DataInputStream(ssTable.openDataStream());

            if (compressionMetadata != null)
            {
                dataStream = CompressedRawInputStream.from(ssTable,
                                                           dataInputStream,
                                                           compressionMetadata,
                                                           stats);
            }
            else
            {
                dataStream = new RawInputStream(dataInputStream, new byte[64 * 1024], stats);
            }
            dis = new DataInputStream(dataStream);
            if (startOffset != null)
            {
                // Skip to start offset, if known, of first in-range partition
                ByteBufferUtils.skipFully(dis, startOffset);
                assert dataStream.position() == startOffset;
                LOGGER.info("Using Data.db start offset to skip ahead startOffset={} sstable='{}'",
                            startOffset, ssTable);
                stats.skippedDataDbStartOffset(startOffset);
            }
            in = new DataInputPlus.DataInputStreamPlus(dis);
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean overlapsSparkTokenRange(BigInteger token)
        {
            return sparkRangeFilter == null || sparkRangeFilter.overlaps(token);
        }

        public boolean overlapsPartitionFilters(DecoratedKey key)
        {
            return partitionKeyFilters.isEmpty()
                || partitionKeyFilters.stream().anyMatch(filter -> filter.matches(key.getKey()));
        }

        public boolean overlaps(DecoratedKey key, BigInteger token)
        {
            return overlapsSparkTokenRange(token) && overlapsPartitionFilters(key);
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                while (true)
                {
                    key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
                    iterator = SSTableSimpleIterator.create(metadata, in, header, helper, partitionLevelDeletion);
                    staticRow = iterator.readStaticRow();
                    BigInteger token = ReaderUtils.tokenToBigInteger(key.getToken());
                    if (overlaps(key, token))
                    {
                        // Partition overlaps with filters
                        long now = System.nanoTime();
                        stats.nextPartition(now - lastTimeNanos);
                        lastTimeNanos = now;
                        return true;
                    }
                    if (lastToken != null && startOffset != null && lastToken.compareTo(token) < 0)
                    {
                        // Partition no longer overlaps SparkTokenRange so we've finished reading this SSTable
                        stats.skippedDataDbEndOffset(dataStream.position() - startOffset);
                        return false;
                    }
                    stats.skippedPartition(key.getKey(), ReaderUtils.tokenToBigInteger(key.getToken()));
                    // Skip partition efficiently without deserializing
                    UnfilteredDeserializer deserializer = UnfilteredDeserializer.create(metadata, in, header, helper);
                    while (deserializer.hasNext())
                    {
                        deserializer.skipNext();
                    }
                }
            }
            catch (EOFException exception)
            {
                return false;
            }
            catch (IOException exception)
            {
                stats.corruptSSTable(exception, metadata.keyspace, metadata.name, ssTable);
                LOGGER.warn("IOException reading sstable keyspace={} table={} dataFileName={} ssTable='{}'",
                            metadata.keyspace, metadata.name, ssTable.getDataFileName(), ssTable, exception);
                throw new SSTableStreamException(exception);
            }
            catch (Throwable throwable)
            {
                stats.corruptSSTable(throwable, metadata.keyspace, metadata.name, ssTable);
                LOGGER.error("Error reading sstable keyspace={} table={}  dataFileName={} ssTable='{}'",
                             metadata.keyspace, metadata.name, ssTable.getDataFileName(), ssTable, throwable);
                throw new RuntimeException(ThrowableUtils.rootCause(throwable));
            }
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return new UnfilteredIterator();
        }

        @Override
        public void close()
        {
            LOGGER.debug("Closing SparkSSTableReader {}", ssTable);
            try
            {
                dis.close();
                if (openedNanos != null)
                {
                    stats.closedSSTable(System.nanoTime() - openedNanos);
                }
            }
            catch (IOException exception)
            {
                LOGGER.warn("IOException closing SSTable DataInputStream", exception);
            }
        }

        @Override
        public long getLengthInBytes()
        {
            // This is mostly used to return Compaction info for Metrics or via JMX so we can ignore here
            return 0;
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            // This is mostly used to return Compaction info for Metrics or via JMX so we can ignore here
            return 0;
        }

        @Override
        public long getBytesScanned()
        {
            return 0;
        }

        @Override
        public Set<org.apache.cassandra.io.sstable.format.SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        private class UnfilteredIterator implements UnfilteredRowIterator
        {
            @Override
            public RegularAndStaticColumns columns()
            {
                return metadata.regularAndStaticColumns();
            }

            @Override
            public TableMetadata metadata()
            {
                return metadata;
            }

            @Override
            public boolean isReverseOrder()
            {
                return false;
            }

            @Override
            public DecoratedKey partitionKey()
            {
                return key;
            }

            @Override
            public DeletionTime partitionLevelDeletion()
            {
                return partitionLevelDeletion;
            }

            @Override
            public Row staticRow()
            {
                return staticRow;
            }

            @Override
            public EncodingStats stats()
            {
                return header.stats();
            }

            @Override
            public boolean hasNext()
            {
                try
                {
                    return iterator.hasNext();
                }
                catch (IOError error)
                {
                    // SSTableSimpleIterator::computeNext wraps IOException in IOError, so we catch those,
                    // try to extract the IOException and re-wrap it in an SSTableStreamException,
                    // which we can then process in TableStreamScanner
                    if (error.getCause() instanceof IOException)
                    {
                        throw new SSTableStreamException((IOException) error.getCause());
                    }

                    // Otherwise, just throw the IOError and deal with it further up the stack
                    throw error;
                }
            }

            @Override
            public Unfiltered next()
            {
                // NOTE: In practice we know that IOException will be thrown by hasNext(),
                //       because that's where the actual reading happens, so we don't bother
                //       catching IOError here (contrarily to what we do in hasNext)
                return iterator.next();
            }

            @Override
            public void close()
            {
            }
        }
    }
}
