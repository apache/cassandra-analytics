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

package org.apache.cassandra.spark.reader.sai;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.SSTableIndexFileAccess;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.plan.SaiPlanAccessor;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.SaiQueryPlanner;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.Preconditions;
import org.apache.cassandra.utils.TokenUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Opens Cassandra 5 SAI components, executes the query plan once, and materializes the resulting candidate tokens
 * as an exact compact token set.
 *
 * <p>Cassandra's native SAI planner analyzes the conjunctive query, including same-column range folding and
 * intersection. For every planned expression, matches are first UNIONed across all SSTables before Cassandra
 * intersects different expressions. This ordering is important for correctness: values contributing to a logically
 * reconciled row may live in different SSTables. Applying conjunctions inside each SSTable could therefore create
 * false negatives.</p>
 *
 * <p>SAI is a read-planning phase only. The native iterator is fully consumed before {@code Data.db} scanning starts,
 * then all SAI resources are closed. The resulting exact tokens are shared by every participating SSTable so the normal
 * Cassandra compaction / reconciliation path can still apply newer values, TTLs and tombstones. Spark keeps the
 * original predicates as residual filters, so stale index entries can only create false positives.</p>
 */
public final class SaiIndexReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaiIndexReader.class);

    private SaiIndexReader()
    {
        throw new IllegalStateException("Static utility class");
    }

    /**
     * Executes all SAI predicates once and returns the sorted candidate tokens as an exact compact token set.
     *
     * @return empty {@link Optional} when SAI cannot safely be used and the caller should fall back to the ordinary SSTable
     *         scan; an empty {@link CandidateTokens} means SAI executed successfully and found no candidates
     */
    @NotNull
    public static Optional<CandidateTokens> findCandidateTokens(@NotNull TableMetadata metadata,
                                                                @NotNull Set<SSTable> sstables,
                                                                @NotNull List<SaiFilter> filters,
                                                                @Nullable SparkRangeFilter sparkRangeFilter,
                                                                int maxCandidateTokens)
    {
        if (sstables.isEmpty() || filters.isEmpty())
        {
            return Optional.of(CandidateTokens.empty());
        }

        ResourceGroup resources = new ResourceGroup();
        try
        {
            SaiQueryPlanner.Plan plan = SaiQueryPlanner.plan(metadata, filters);
            if (plan == null)
            {
                return Optional.empty();
            }

            List<StorageAttachedIndex> resourceIndexes = plan.resourceIndexes();

            List<SSTableResources> sstableResources = new ArrayList<>(sstables.size());
            for (SSTable sstable : sstables)
            {
                SSTableResources opened = SSTableResources.open(metadata, sstable, resourceIndexes);
                resources.add(opened);
                sstableResources.add(opened);
            }

            QueryContext queryContext = new QueryContext(null, Long.MAX_VALUE);
            try (KeyRangeIterator finalMatches = executePlan(metadata,
                                                             plan,
                                                             sstableResources,
                                                             queryContext,
                                                             sparkRangeFilter))
            {
                finalMatches.setOnClose(resources::close);
                seekToSparkRangeStart(finalMatches,
                                      sparkRangeFilter,
                                      metadata.partitioner,
                                      metadata.comparator);
                return collectCandidateTokens(finalMatches,
                                              sparkRangeFilter,
                                              metadata.partitioner,
                                              maxCandidateTokens);
            }
        }
        catch (Exception exception)
        {
            resources.close();
            // No Data.db rows have been emitted yet, so every SAI failure can safely fail open to a normal scan.
            LOGGER.warn("Unable to use SAI for SSTable pruning, falling back to normal SSTable scan", exception);
            return Optional.empty();
        }
    }

    /**
     * Fully consumes the sorted native SAI result stream and converts it to the exact token-set representation
     * used by the Data.db read-planning phase.
     */
    @NotNull
    static Optional<CandidateTokens> collectCandidateTokens(@NotNull Iterator<PrimaryKey> matches,
                                                            @Nullable SparkRangeFilter sparkRangeFilter,
                                                            @NotNull IPartitioner partitioner,
                                                            int maxCandidateTokens)
    {
        Preconditions.checkArgument(maxCandidateTokens >= 0, "maxCandidateTokens must be non-negative");
        BigInteger upperToken = sparkRangeFilter == null
                                ? null
                                : sparkRangeFilter.tokenRange().upperEndpoint();
        CandidateTokens.Builder candidates = CandidateTokens.builder(partitioner);
        while (matches.hasNext())
        {
            DecoratedKey partitionKey = matches.next().partitionKey();
            BigInteger token = TokenUtils.tokenToBigInteger(partitionKey.getToken());
            // SAI iterators are sorted by primary key/token. Once the worker's upper
            // token is crossed there cannot be another candidate belonging to this task.
            if (upperToken != null && token.compareTo(upperToken) > 0)
            {
                break;
            }
            if (sparkRangeFilter == null || !sparkRangeFilter.skipPartition(token))
            {
                candidates.add(token);
                if (candidates.size() > maxCandidateTokens)
                {
                    LOGGER.info("SAI matched more than {} unique candidate partition tokens; " +
                                "falling back to normal SSTable scan. Configure 'saiMaxCandidateTokens' " +
                                "option to change the limit", maxCandidateTokens);
                    return Optional.empty();
                }
            }
        }
        return Optional.of(candidates.build());
    }

    /**
     * Positions the SAI iterator at the beginning of the token range assigned to this Spark task.
     */
    static void seekToSparkRangeStart(@NotNull KeyRangeIterator matches,
                                      @Nullable SparkRangeFilter sparkRangeFilter,
                                      @NotNull IPartitioner partitioner,
                                      @NotNull ClusteringComparator comparator)
    {
        if (sparkRangeFilter == null)
        {
            return;
        }

        // The worker range is open on the left ((lower, upper]). Seeking to 'lower' is intentional:
        // SAI's token-only primary key gives us an efficient lower-bound seek, while the existing range check in
        // collectCandidateTokens(Iterator, SparkRangeFilter, IPartitioner, int) still rejects any partition
        // whose token is exactly equal to the open lower bound.
        BigInteger lowerEndpoint = sparkRangeFilter.tokenRange().lowerEndpoint();
        Token lowerToken = partitioner.getTokenFactory().fromString(lowerEndpoint.toString());
        matches.skipTo(new PrimaryKey.Factory(partitioner, comparator).create(lowerToken));
    }

    static boolean overlapsSparkRange(@NotNull PrimaryKey minimumKey,
                                      @NotNull PrimaryKey maximumKey,
                                      @Nullable SparkRangeFilter sparkRangeFilter)
    {
        if (sparkRangeFilter == null)
        {
            return true;
        }

        BigInteger minimumToken = TokenUtils.tokenToBigInteger(minimumKey.token());
        BigInteger maximumToken = TokenUtils.tokenToBigInteger(maximumKey.token());
        return sparkRangeFilter.overlaps(TokenRange.closed(minimumToken, maximumToken));
    }

    @NotNull
    private static KeyRangeIterator executePlan(@NotNull TableMetadata metadata,
                                                @NotNull SaiQueryPlanner.Plan plan,
                                                @NotNull List<SSTableResources> sstableResources,
                                                @NotNull QueryContext queryContext,
                                                @Nullable SparkRangeFilter sparkRangeFilter)
    {
        QueryController controller = new AnalyticsQueryController(metadata, plan, sstableResources, queryContext, sparkRangeFilter);
        // Cassandra's Operation planner owns expression analysis, same-column range folding, and intersection.
        return SaiPlanAccessor.buildIterator(controller);
    }

    /**
     * Supplies Cassandra's native SAI planner with the Analytics-owned SSTable search implementation.
     *
     * <p>Native QueryController builds a QueryView from live Cassandra SSTableReader instances. Analytics has
     * remote / offline SSTables instead, so index selection and expression planning remain native while this adapter
     * replaces only the step that turns planned Expressions into KeyRangeIterators.</p>
     */
    private static final class AnalyticsQueryController extends QueryController
    {
        private final SaiQueryPlanner.Plan plan;
        private final List<SSTableResources> sstableResources;
        private final QueryContext queryContext;
        private final SparkRangeFilter sparkRangeFilter;

        private AnalyticsQueryController(@NotNull TableMetadata metadata,
                                         @NotNull SaiQueryPlanner.Plan plan,
                                         @NotNull List<SSTableResources> sstableResources,
                                         @NotNull QueryContext queryContext,
                                         @Nullable SparkRangeFilter sparkRangeFilter)
        {
            super(columnFamilyStore(metadata),
                  PartitionRangeReadCommand.allDataRead(metadata, 0L),
                  plan.rowFilter(),
                  queryContext);
            this.plan = plan;
            this.sstableResources = sstableResources;
            this.queryContext = queryContext;
            this.sparkRangeFilter = sparkRangeFilter;
        }

        @Nullable
        @Override
        public StorageAttachedIndex indexFor(RowFilter.Expression expression)
        {
            return plan.indexFor(expression.column());
        }

        @Override
        public boolean usesStrictFiltering()
        {
            return true;
        }

        @Override
        public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
        {
            KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder(expressions.size(), 0);
            List<KeyRangeIterator> expressionMatches = new ArrayList<>(expressions.size());
            try
            {
                for (Expression expression : expressions)
                {
                    KeyRangeIterator matches = searchAcrossSSTables(expression);
                    expressionMatches.add(matches);
                    builder.add(matches);
                }
                return builder;
            }
            catch (Exception exception)
            {
                closeAll(expressionMatches);
                throw new RuntimeException("Unable to search in SAI SSTables", exception);
            }
        }

        @NotNull
        private KeyRangeIterator searchAcrossSSTables(@NotNull Expression expression) throws IOException
        {
            List<KeyRangeIterator> perSSTableMatches = new ArrayList<>(sstableResources.size());
            try
            {
                for (SSTableResources resource : sstableResources)
                {
                    perSSTableMatches.add(resource.search(expression, queryContext, sparkRangeFilter));
                }
                // A logical row may be assembled from values written in different SSTables. Always UNION one
                // planned expression across SSTables before intersecting different expressions.
                return KeyRangeUnionIterator.build(perSSTableMatches);
            }
            catch (Exception exception)
            {
                closeAll(perSSTableMatches);
                throw new RuntimeException("Unable to execute SAI expression", exception);
            }
        }
    }

    @NotNull
    private static ColumnFamilyStore columnFamilyStore(@NotNull TableMetadata metadata)
    {
        return Keyspace.openWithoutSSTables(metadata.keyspace).getColumnFamilyStore(metadata.name);
    }

    /**
     * Holds all open native SAI resources for one SSTable.
     */
    private static final class SSTableResources implements Closeable
    {
        private final Descriptor descriptor;
        private final IndexDescriptor indexDescriptor;
        private final PrimaryKeyMap.Factory primaryKeyMapFactory;
        private final Map<StorageAttachedIndex, OpenColumnIndex> columns = new HashMap<>();
        private boolean closed;

        private SSTableResources(Descriptor descriptor,
                                 IndexDescriptor indexDescriptor,
                                 PrimaryKeyMap.Factory primaryKeyMapFactory)
        {
            this.descriptor = descriptor;
            this.indexDescriptor = indexDescriptor;
            this.primaryKeyMapFactory = primaryKeyMapFactory;
        }

        @NotNull
        static SSTableResources open(@NotNull TableMetadata metadata,
                                     @NotNull SSTable sstable,
                                     @NotNull List<StorageAttachedIndex> indexes) throws IOException
        {
            if (sstable.customComponentNames().isEmpty())
            {
                throw new IOException("SSTable has no SAI components: " + sstable.getDataFileName());
            }

            SSTableResources resources = null;
            try
            {
                // Descriptor is still needed for  SAI naming / ID logic, but this path is only an identifier.
                File dataFile = new File(".", sstable.getDataFileName());
                Descriptor descriptor = Descriptor.fromFileWithComponent(dataFile, metadata.keyspace, metadata.name).left;
                IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator,
                                                                         new SSTableIndexFileAccess(sstable));

                if (!indexDescriptor.isPerSSTableIndexBuildComplete())
                {
                    throw new IOException("Incomplete per-SSTable SAI components: " + sstable.getDataFileName());
                }
                for (StorageAttachedIndex index : indexes)
                {
                    if (!indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()))
                    {
                        throw new IOException("Incomplete SAI components for " + index.identifier()
                                              + " in " + sstable.getDataFileName());
                    }
                }

                resources = new SSTableResources(descriptor,
                                                 indexDescriptor,
                                                 indexDescriptor.newPrimaryKeyMapFactory(null));
                for (StorageAttachedIndex index : indexes)
                {
                    resources.openColumn(index);
                }
                return resources;
            }
            catch (Exception exception)
            {
                if (resources != null)
                {
                    resources.close();
                }
                throw new IOException("Unable to open SAI components for " + sstable.getDataFileName(), exception);
            }
        }

        private void openColumn(StorageAttachedIndex index) throws IOException
        {
            if (indexDescriptor.isIndexEmpty(index.termType(), index.identifier()))
            {
                columns.put(index, OpenColumnIndex.empty());
                return;
            }

            PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor,
                                                                     index.termType(),
                                                                     index.identifier());
            OpenColumnIndex column = new OpenColumnIndex(indexFiles);
            try
            {
                MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
                List<SegmentMetadata> segments = SegmentMetadata.load(metadataSource, indexDescriptor.primaryKeyFactory);
                for (SegmentMetadata segment : segments)
                {
                    column.segments.add(new OpenSegment(segment,
                                                        IndexSegmentSearcher.open(primaryKeyMapFactory,
                                                                                  descriptor.id,
                                                                                  indexFiles,
                                                                                  segment,
                                                                                  index)));
                }
                columns.put(index, column);
            }
            catch (Exception exception)
            {
                column.close();
                throw new IOException("Unable to open SAI column " + index.identifier(), exception);
            }
        }

        @NotNull
        KeyRangeIterator search(@NotNull Expression expression,
                                @NotNull QueryContext queryContext,
                                @Nullable SparkRangeFilter sparkRangeFilter) throws IOException
        {
            Preconditions.checkState(!expression.isNotIndexed(), "SAI planner produced an unindexed expression: " + expression);
            StorageAttachedIndex index = expression.getIndex();
            OpenColumnIndex column = columns.get(index);
            if (column == null || column.segments.isEmpty())
            {
                return KeyRangeIterator.empty();
            }

            List<KeyRangeIterator> segmentMatches = new ArrayList<>(column.segments.size());
            try
            {
                for (OpenSegment segment : column.segments)
                {
                    // Segment metadata is ordered by primary key. Avoid opening / searching a segment
                    // whose complete token span is outside the token range assigned to this Spark worker.
                    if (!overlapsSparkRange(segment.metadata.minKey, segment.metadata.maxKey, sparkRangeFilter))
                    {
                        continue;
                    }
                    Bounds<PartitionPosition> keyRange = new Bounds<>(segment.metadata.minKey.partitionKey(),
                                                                      segment.metadata.maxKey.partitionKey());
                    segmentMatches.add(segment.searcher.search(expression, keyRange, queryContext));
                }
                return KeyRangeUnionIterator.build(segmentMatches);
            }
            catch (Exception exception)
            {
                closeAll(segmentMatches);
                throw new IOException("Unable to search SAI column " + index.identifier(), exception);
            }
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            closeAll(columns.values());
            columns.clear();
            closeQuietly(primaryKeyMapFactory);
        }
    }

    private static final class OpenColumnIndex implements Closeable
    {
        @Nullable
        private final PerColumnIndexFiles indexFiles;
        private final List<OpenSegment> segments = new ArrayList<>();
        private boolean closed;

        private OpenColumnIndex(@Nullable PerColumnIndexFiles indexFiles)
        {
            this.indexFiles = indexFiles;
        }

        static OpenColumnIndex empty()
        {
            return new OpenColumnIndex(null);
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            for (OpenSegment segment : segments)
            {
                closeQuietly(segment.searcher);
            }
            segments.clear();
            closeQuietly(indexFiles);
        }
    }

    private static final class OpenSegment
    {
        private final SegmentMetadata metadata;
        private final IndexSegmentSearcher searcher;

        private OpenSegment(SegmentMetadata metadata, IndexSegmentSearcher searcher)
        {
            this.metadata = metadata;
            this.searcher = searcher;
        }
    }

    /**
     * Owns all per-SSTable resources and closes them exactly once.
     */
    private static final class ResourceGroup implements Closeable
    {
        private final List<SSTableResources> resources = new ArrayList<>();
        private boolean closed;

        void add(SSTableResources resource)
        {
            resources.add(resource);
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            closeAll(resources);
            resources.clear();
        }
    }

    private static void closeAll(Collection<? extends Closeable> closeables)
    {
        for (Closeable closeable : closeables)
        {
            closeQuietly(closeable);
        }
    }

    private static void closeQuietly(@Nullable Closeable closeable)
    {
        if (closeable == null)
        {
            return;
        }
        try
        {
            closeable.close();
        }
        catch (IOException exception)
        {
            LOGGER.debug("Failed to close SAI resource", exception);
        }
    }
}
