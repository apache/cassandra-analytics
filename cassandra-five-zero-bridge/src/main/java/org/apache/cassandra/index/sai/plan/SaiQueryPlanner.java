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
package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts version-neutral Spark SAI predicates to Cassandra's native SAI query planner.
 *
 * <p>The input is a flat conjunction because Cassandra 5.0 SAI does not support OR. This class only creates the
 * {@link RowFilter} and matching index instances; Cassandra's {@code Operation} planner owns expression analysis,
 * same-column range folding, and intersection planning.</p>
 */
public class SaiQueryPlanner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaiQueryPlanner.class);

    private SaiQueryPlanner()
    {
        throw new IllegalStateException("Static utility class");
    }

    @Nullable
    public static Plan plan(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        if (filters.isEmpty())
        {
            return null;
        }

        Map<String, StorageAttachedIndex> indexesCache = new HashMap<>();
        RowFilter rowFilter = RowFilter.create(false);
        Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn = new LinkedHashMap<>();
        for (SaiFilter filter : filters)
        {
            StorageAttachedIndex index = storageAttachedIndex(indexesCache, metadata, filter.index());
            Operator operator = operator(filter.operator());
            if (!canSearch(index, operator))
            {
                return null;
            }

            ColumnMetadata column = index.termType().columnMetadata();
            StorageAttachedIndex previous = indexesByColumn.putIfAbsent(column, index);
            if (previous != null && previous != index)
            {
                LOGGER.warn("Unclear index choice for column {}, candidates: {}, {}. Falling back to full table scan.",
                            column, previous, index);
                return null;
            }
            rowFilter.add(column, operator, index.termType().fromString(filter.value()));
        }
        return new Plan(rowFilter, indexesByColumn);
    }

    private static boolean canSearch(@NotNull StorageAttachedIndex index, @NotNull Operator operator)
    {
        return !index.hasAnalyzer()
               && !index.termType().isVector()
               && !index.termType().isNonFrozenCollection()
               && !index.termType().isFrozenCollection()
               && !index.termType().isComposite()
               && index.supportsExpression(index.termType().columnMetadata(), operator);
    }

    @NotNull
    private static StorageAttachedIndex storageAttachedIndex(@NotNull Map<String, StorageAttachedIndex> indexesCache,
                                                             @NotNull TableMetadata metadata,
                                                             @NotNull SaiIndex saiIndex)
    {
        String cacheKey = metadata.id + ":" + saiIndex.name();
        return indexesCache.computeIfAbsent(cacheKey, ignored -> {
            ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(metadata.keyspace).getColumnFamilyStore(metadata.name);
            Map<String, String> options = new HashMap<>(saiIndex.options());
            options.put(IndexTarget.TARGET_OPTION_NAME, saiIndex.target());
            options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(saiIndex.name(),
                                                                           IndexMetadata.Kind.CUSTOM,
                                                                           options);
            return new StorageAttachedIndex(cfs, indexMetadata);
        });
    }

    @NotNull
    private static Operator operator(@NotNull SaiFilter.Operator operator)
    {
        switch (operator)
        {
            case EQ:
                return Operator.EQ;
            case LT:
                return Operator.LT;
            case LTE:
                return Operator.LTE;
            case GT:
                return Operator.GT;
            case GTE:
                return Operator.GTE;
            default:
                throw new IllegalArgumentException("Unsupported SAI operator: " + operator);
        }
    }

    public static final class Plan
    {
        private final RowFilter rowFilter;
        private final Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn;
        private final List<StorageAttachedIndex> resourceIndexes;

        private Plan(@NotNull RowFilter rowFilter,
                     @NotNull Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn)
        {
            this.rowFilter = rowFilter;
            this.indexesByColumn = Collections.unmodifiableMap(new LinkedHashMap<>(indexesByColumn));
            this.resourceIndexes = Collections.unmodifiableList(new ArrayList<>(indexesByColumn.values()));
        }

        @NotNull
        public RowFilter rowFilter()
        {
            return rowFilter;
        }

        @Nullable
        public StorageAttachedIndex indexFor(@NotNull ColumnMetadata column)
        {
            return indexesByColumn.get(column);
        }

        @NotNull
        public List<StorageAttachedIndex> resourceIndexes()
        {
            return resourceIndexes;
        }
    }
}
