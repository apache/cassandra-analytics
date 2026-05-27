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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Helper methods for managing bloom filters.
 */
public class FilterDbUtils
{
    private FilterDbUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static IFilter buildBloomFilter(@NotNull CqlTable cqltable,
                                           @NotNull SSTable ssTable,
                                           @NotNull TableMetadata tableMetadata) throws IOException
    {
        String keyspace = cqltable.keyspace();
        String table = cqltable.table();

        Map<MetadataType, MetadataComponent> componentMap = ReaderUtils.deserializeStatsMetadata(keyspace, table, ssTable, EnumSet.of(MetadataType.STATS));
        StatsMetadata statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);

        return FilterFactory.getFilter(statsMetadata.totalRows, tableMetadata.params.bloomFilterFpChance);
    }
}
