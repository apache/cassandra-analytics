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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.RebufferingChannelInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.Nullable;

public class IndexSummaryComponent implements AutoCloseable
{
    private final IndexSummary indexSummary;
    private final DecoratedKey firstKey;
    private final DecoratedKey lastKey;

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
    @Nullable
    static IndexSummaryComponent readSummary(InputStream summaryStream,
                                             IPartitioner partitioner,
                                             int minIndexInterval,
                                             int maxIndexInterval) throws IOException
    {
        if (summaryStream == null)
        {
            return null;
        }

        int bufferSize = ReaderUtils.inputStreamBufferSize(summaryStream);
        try (DataInputStream is = new DataInputStream(summaryStream);
             DataInputPlus.DataInputStreamPlus dis = new RebufferingChannelInputStream(is, bufferSize))
        {
            IndexSummary indexSummary = IndexSummary.serializer.deserialize(dis, partitioner, minIndexInterval, maxIndexInterval);
            DecoratedKey firstKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(dis));
            DecoratedKey lastKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(dis));
            return new IndexSummaryComponent(indexSummary, firstKey, lastKey);
        }
    }

    IndexSummaryComponent(IndexSummary indexSummary,
                          DecoratedKey firstKey,
                          DecoratedKey lastKey)
    {
        this.indexSummary = indexSummary;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    /**
     * Get a shared copy of the IndexSummary, whose reference count is incremented.
     * It is important to close the shared copy to decrement the reference count.
     * @return a shared copy of the IndexSummary object
     */
    public IndexSummary summarySharedCopy()
    {
        return indexSummary.sharedCopy();
    }

    public DecoratedKey first()
    {
        return firstKey;
    }

    public DecoratedKey last()
    {
        return lastKey;
    }

    @Override // The method is expected to be called when evicting the object from sstable cache; do not call it explicitly.
    public void close() throws Exception
    {
        indexSummary.close();
    }
}
