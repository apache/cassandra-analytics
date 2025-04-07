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

import java.io.InputStream;
import java.math.BigInteger;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.bti.BtiReaderUtils;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.IncompleteSSTableException;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.common.IIndexReader;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BtiIndexReader implements IIndexReader
{
    private TokenRange ssTableRange = null;

    public BtiIndexReader(@NotNull SSTable ssTable,
                          @NotNull TableMetadata metadata,
                          @Nullable SparkRangeFilter rangeFilter,
                          @NotNull Stats stats,
                          @NotNull IndexConsumer consumer)
    {
        long now = System.nanoTime();
        long startTimeNanos = now;
        try
        {
            File file = ReaderUtils.constructFilename(metadata.keyspace, metadata.name, ssTable.getDataFileName());
            Descriptor descriptor = Descriptor.fromFileWithComponent(file, false).left;

            now = System.nanoTime();

            // read through Index.db and consume Partition keys
            try (InputStream is = ssTable.openPrimaryIndexStream())
            {
                if (is == null)
                {
                    consumer.onFailure(new IncompleteSSTableException(FileType.INDEX));
                    return;
                }

                BtiReaderUtils.consumePrimaryIndex(ssTable,
                                                   metadata,
                                                   descriptor,
                                                   rangeFilter,
                                                   consumer);
                stats.indexFileRead(System.nanoTime() - now);
            }
        }
        catch (Throwable t)
        {
            consumer.onFailure(t);
        }
        finally
        {
            consumer.onFinished(System.nanoTime() - startTimeNanos);
        }
    }

    public BigInteger firstToken()
    {
        return ssTableRange != null ? ssTableRange.firstEnclosedValue() : null;
    }

    public BigInteger lastToken()
    {
        return ssTableRange != null ? ssTableRange.upperEndpoint() : null;
    }

    public boolean ignore()
    {
        return false;
    }
}
