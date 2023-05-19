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

package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.reader.CdcScannerBuilder;
import org.apache.cassandra.spark.reader.Scannable;
import org.jetbrains.annotations.NotNull;

public class PartitionUpdateWrapper implements IPartitionUpdateWrapper, Scannable, Comparable<PartitionUpdateWrapper>
{
    public final String keyspace;
    public final String table;
    private final TableMetadata tableMetadata;
    private final PartitionUpdate update;
    private final ByteBuffer digestBytes;
    private final long maxTimestampMicros;
    private final int dataSize;

    public PartitionUpdateWrapper(TableMetadata tableMetadata, PartitionUpdate update, long maxTimestampMicros)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.keyspace = update.metadata().keyspace;
        this.table = update.metadata().name;
        this.maxTimestampMicros = maxTimestampMicros;
        Digest digest = Digest.forReadResponse();
        UnfilteredRowIterators.digest(update.unfilteredIterator(), digest, MessagingService.current_version);
        this.digestBytes = ByteBuffer.wrap(digest.digest());
        this.dataSize = 18 /* = 8 + 4 + 2 + 4 */ + digestBytes.remaining() + update.dataSize();
    }

    // For deserialization
    private PartitionUpdateWrapper(TableMetadata tableMetadata,
                                   PartitionUpdate update,
                                   String keyspace,
                                   String table,
                                   long maxTimestampMicros,
                                   ByteBuffer digestBytes,
                                   int dataSize)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.keyspace = keyspace;
        this.table = table;
        this.maxTimestampMicros = maxTimestampMicros;
        this.digestBytes = digestBytes;
        this.dataSize = dataSize;
    }

    public DecoratedKey partitionKey()
    {
        return update.partitionKey();
    }

    @Override
    public long maxTimestampMicros()
    {
        return maxTimestampMicros;
    }

    @Override
    public int dataSize()
    {
        return dataSize;
    }

    public PartitionUpdate partitionUpdate()
    {
        return update;
    }

    public ByteBuffer digest()
    {
        return digestBytes;
    }

    @Override
    public ISSTableScanner scanner()
    {
        return new CdcScannerBuilder.CdcScanner(tableMetadata, update);
    }

    // TODO: Add proper equals and hashCode impl for PartitionUpdate in OSS
    @Override
    public int hashCode()
    {
        return digestBytes.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        else if (other instanceof PartitionUpdateWrapper)
        {
            return this.digestBytes.equals(((PartitionUpdateWrapper) other).digestBytes);
        }
        else
        {
            return false;
        }
    }

    public int compareTo(@NotNull PartitionUpdateWrapper that)
    {
        return Long.compare(this.maxTimestampMicros, that.maxTimestampMicros);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<PartitionUpdateWrapper>
    {
        final TableMetadata metadata;
        final boolean includePartitionUpdate;

        public Serializer(String keyspace, String table)
        {
            this(keyspace, table, false);
        }

        public Serializer(String keyspace, String table, boolean includePartitionUpdate)
        {
            this.metadata = Schema.instance.getTableMetadata(keyspace, table);
            this.includePartitionUpdate = includePartitionUpdate;
        }

        public Serializer(TableMetadata metadata)
        {
            this(metadata, false);
        }

        public Serializer(TableMetadata metadata, boolean includePartitionUpdate)
        {
            this.metadata = metadata;
            this.includePartitionUpdate = includePartitionUpdate;
        }

        @Override
        public PartitionUpdateWrapper read(Kryo kryo, Input in, Class type)
        {
            long maxTimestampMicros = in.readLong();
            int size = in.readInt();

            // Read digest
            ByteBuffer digest = ByteBuffer.wrap(in.readBytes(in.readShort()));

            PartitionUpdate partitionUpdate = null;
            if (includePartitionUpdate)
            {
                // Read partition update
                partitionUpdate = PartitionUpdate.fromBytes(ByteBuffer.wrap(in.readBytes(in.readInt())), MessagingService.current_version);
            }

            String keyspace = in.readString();
            String table = in.readString();

            return new PartitionUpdateWrapper(metadata, partitionUpdate, keyspace, table, maxTimestampMicros, digest, size);
        }

        @Override
        public void write(Kryo kryo, Output out, PartitionUpdateWrapper update)
        {
            out.writeLong(update.maxTimestampMicros);  // 8 bytes
            out.writeInt(update.dataSize());           // 4 bytes

            // Write digest
            byte[] bytes = new byte[update.digestBytes.remaining()];
            update.digestBytes.get(bytes);
            out.writeShort(bytes.length);              // 2 bytes
            out.writeBytes(bytes);                     // Variable bytes
            update.digestBytes.clear();

            // Write partition update
            if (includePartitionUpdate)
            {
                ByteBuffer buffer = PartitionUpdate.toBytes(update.update, MessagingService.current_version);
                byte[] array = new byte[buffer.remaining()];
                buffer.get(array);
                out.writeInt(array.length);            // 4 bytes
                out.writeBytes(array);                 // Variable bytes
            }

            out.writeString(update.keyspace);
            out.writeString(update.table);
        }
    }
}
