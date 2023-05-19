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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;

/**
 * Helper methods for reading the Summary.db SSTable file component
 */
final class SummaryDbUtils
{
    static class Summary
    {
        private final IndexSummary indexSummary;
        private final DecoratedKey firstKey;
        private final DecoratedKey lastKey;

        Summary(IndexSummary indexSummary,
                DecoratedKey firstKey,
                DecoratedKey lastKey)
        {
            this.indexSummary = indexSummary;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        public IndexSummary summary()
        {
            return indexSummary;
        }

        public DecoratedKey first()
        {
            return firstKey;
        }

        public DecoratedKey last()
        {
            return lastKey;
        }
    }

    private SummaryDbUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    static Summary readSummary(@NotNull TableMetadata metadata, @NotNull SSTable ssTable) throws IOException
    {
        try (InputStream in = ssTable.openSummaryStream())
        {
            return readSummary(in, metadata.partitioner, metadata.params.minIndexInterval, metadata.params.maxIndexInterval);
        }
    }

    /**
     * Read and deserialize the Summary.db file
     *
     * @param summaryStream    input stream for Summary.db file
     * @param partitioner      token partitioner
     * @param minIndexInterval min index interval
     * @param maxIndexInterval max index interval
     * @return Summary object
     * @throws IOException io exception
     */
    static Summary readSummary(InputStream summaryStream,
                               IPartitioner partitioner,
                               int minIndexInterval,
                               int maxIndexInterval) throws IOException
    {
        if (summaryStream == null)
        {
            return null;
        }

        try (DataInputStream is = new DataInputStream(summaryStream))
        {
            IndexSummary indexSummary = IndexSummary.serializer.deserialize(is, partitioner, minIndexInterval, maxIndexInterval);
            DecoratedKey firstKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(is));
            DecoratedKey lastKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(is));
            return new Summary(indexSummary, firstKey, lastKey);
        }
    }

    public interface TokenList
    {
        int size();

        BigInteger tokenAt(int index);
    }

    /**
     * Binary search Summary.db to find nearest offset in Index.db that precedes the token we are looking for
     *
     * @param summary     IndexSummary from Summary.db file
     * @param partitioner Cassandra partitioner to hash partition keys to token
     * @param token       the token we are trying to find
     * @return offset into the Index.db file for the closest to partition in the Summary.db file that precedes the token we are looking for
     */
    public static long findIndexOffsetInSummary(IndexSummary summary, IPartitioner partitioner, BigInteger token)
    {
        return summary.getPosition(binarySearchSummary(summary, partitioner, token));
    }

    public static class IndexSummaryTokenList implements TokenList
    {
        final IPartitioner partitioner;
        final IndexSummary summary;

        IndexSummaryTokenList(IPartitioner partitioner,
                              IndexSummary summary)
        {
            this.partitioner = partitioner;
            this.summary = summary;
        }

        public int size()
        {
            return summary.size();
        }

        public BigInteger tokenAt(int index)
        {
            return ReaderUtils.tokenToBigInteger(partitioner.decorateKey(ByteBuffer.wrap(summary.getKey(index))).getToken());
        }
    }

    public static int binarySearchSummary(IndexSummary summary, IPartitioner partitioner, BigInteger token)
    {
        return binarySearchSummary(new IndexSummaryTokenList(partitioner, summary), token);
    }

    /**
     * Binary search the Summary.db file to find nearest index offset in Index.db for a given token.
     * Method lifted from org.apache.cassandra.io.sstable.IndexSummary.binarySearch(PartitionPosition key) and reworked for tokens.
     *
     * @param tokenList list of tokens to binary search
     * @param token     token to find
     * @return closest offset in Index.db preceding token
     */
    public static int binarySearchSummary(TokenList tokenList, BigInteger token)
    {
        int low = 0;
        int mid = tokenList.size();
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = low + high >> 1;
            result = token.compareTo(tokenList.tokenAt(mid));
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result < 0)
            {
                high = mid - 1;
            }
            else
            {
                break;  // Exact match
            }
        }

        // If:
        //  1) result < 0: the token is less than nearest sampled token found at mid, so we need to start from mid - 1.
        //  2) result == 0: we found an exact match for the token in the sample,
        //     but there may be token collisions in Data.db so start from mid -1 to be safe.
        //  3) result > 0: the nearest sample token at mid is less than the token so we can start from that position.
        return result <= 0 ? Math.max(0, mid - 1) : mid;
    }
}
