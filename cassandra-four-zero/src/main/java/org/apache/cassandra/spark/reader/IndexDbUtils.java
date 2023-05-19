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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Helper methods for reading the Index.db SSTable file component
 */
final class IndexDbUtils
{
    private IndexDbUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @Nullable
    public static Long findDataDbOffset(@NotNull IndexSummary indexSummary,
                                        @NotNull TokenRange range,
                                        @NotNull IPartitioner partitioner,
                                        @NotNull SSTable ssTable,
                                        @NotNull Stats stats) throws IOException
    {
        long searchStartOffset = SummaryDbUtils.findIndexOffsetInSummary(indexSummary, partitioner, range.lowerEndpoint());

        // Open the Index.db, skip to nearest offset found in Summary.db and find start & end offset for the Data.db file
        return findDataDbOffset(range, partitioner, ssTable, stats, searchStartOffset);
    }

    @Nullable
    public static Long findDataDbOffset(@NotNull TokenRange range,
                                        @NotNull IPartitioner partitioner,
                                        @NotNull SSTable ssTable,
                                        @NotNull Stats stats,
                                        long searchStartOffset) throws IOException
    {
        try (InputStream is = ssTable.openPrimaryIndexStream())
        {
            return findIndexOffset(is, partitioner, range, stats, searchStartOffset);
        }
    }

    /**
     * Find the first Data.db offset in the Index.db file for a given token range,
     * using the approximate start offset found in the Summary.db file to seek ahead to the nearest position in the Index.db file
     *
     * @param is                the input stream on the Index.db file
     * @param partitioner       Cassandra partitioner
     * @param range             the range we are trying to find
     * @param stats             stats instance
     * @param searchStartOffset the Index.db approximate start offset read from the Summary.db sample file
     * @return the index offset into the Data.db file for the first partition greater than or equal to the token, or null if cannot find
     * @throws IOException IOException reading Index.db file
     */
    @Nullable
    static Long findIndexOffset(@Nullable InputStream is,
                                @NotNull IPartitioner partitioner,
                                @NotNull TokenRange range,
                                @NotNull Stats stats,
                                long searchStartOffset) throws IOException
    {
        if (is == null)
        {
            return null;
        }

        try
        {
            // Skip to Index.db offset found in Summary.db file
            DataInputStream in = new DataInputStream(is);
            ByteBufferUtils.skipFully(in, searchStartOffset);

            return findStartOffset(in, partitioner, range, stats);
        }
        catch (EOFException ignore)
        {
            // We can possibly reach EOF before start has been found, which is fine
        }

        return null;
    }

    /**
     * Find and return Data.db offset for first overlapping partition
     *
     * @param in          Index.db DataInputStream
     * @param partitioner partitioner
     * @param range       Spark worker token range
     * @param stats       stats instance
     * @return start offset into the Data.db file for the first overlapping partition
     * @throws IOException IOException reading Index.db file
     */
    static long findStartOffset(@NotNull DataInputStream in,
                                @NotNull IPartitioner partitioner,
                                @NotNull TokenRange range,
                                @NotNull Stats stats) throws IOException
    {
        BigInteger keyToken;
        long previous = 0L;
        // CHECKSTYLE IGNORE: An idiomatic way to read input streams
        while (isLessThan(keyToken = readNextToken(partitioner, in, stats), range))
        {
            // Keep skipping until we find first partition overlapping with Spark token range
            previous = ReaderUtils.readPosition(in);
            ReaderUtils.skipPromotedIndex(in);
        }
        assert range.lowerEndpoint().compareTo(keyToken) <= 0;
        // Found first token that overlaps with Spark token range because we passed the target
        // by skipping the promoted index, we use the previously-read position as start
        return previous;
    }

    /**
     * @param keyToken key token read from Index.db
     * @param range    spark worker token range
     * @return true if keyToken is less than the range lower bound
     */
    static boolean isLessThan(@NotNull BigInteger keyToken, @NotNull TokenRange range)
    {
        return keyToken.compareTo(range.lowerEndpoint()) < 0;
    }

    /**
     * Read partition key, use partitioner to hash and return token as BigInteger
     *
     * @param partitioner partitioner
     * @param in          Index.db DataInputStream
     * @param stats       stats instance
     * @return token as BigInteger
     * @throws IOException IOException reading Index.db file
     */
    static BigInteger readNextToken(@NotNull IPartitioner partitioner,
                                    @NotNull DataInputStream in,
                                    @NotNull Stats stats) throws IOException
    {
        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
        BigInteger token = ReaderUtils.tokenToBigInteger(partitioner.decorateKey(key).getToken());
        stats.readPartitionIndexDb((ByteBuffer) key.rewind(), token);
        return token;
    }
}
