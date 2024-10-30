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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.spark.utils.RangeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PartitionUpdateWrapper implements Comparable<PartitionUpdateWrapper>
{
    public static final PartitionUpdateWrapper.DigestSerializer DIGEST_SERIALIZER = new PartitionUpdateWrapper.DigestSerializer();

    @NotNull
    private final ByteBuffer partitionKey;
    private final Digest digest;

    public PartitionUpdateWrapper(String keyspace,
                                  String table,
                                  BigInteger token,
                                  int dataSize,
                                  Supplier<byte[]> digestSupplier,
                                  @NotNull ByteBuffer partitionKey,
                                  long maxTimestampMicros,
                                  @Nullable AsyncExecutor executor)
    {
        this(partitionKey, new Digest(keyspace, table, token, dataSize, digestSupplier, maxTimestampMicros, executor));
    }

    public PartitionUpdateWrapper(@NotNull ByteBuffer partitionKey, Digest digest)
    {
        this.partitionKey = partitionKey;
        this.digest = digest;
    }

    @Nullable
    public ByteBuffer partitionKey()
    {
        return partitionKey;
    }

    public Digest digest()
    {
        return digest;
    }

    public int dataSize()
    {
        return digest.dataSize();
    }

    public String keyspace()
    {
        return digest.keyspace;
    }

    public String table()
    {
        return digest.table;
    }

    public BigInteger token()
    {
        return digest.token;
    }

    public long maxTimestampMicros()
    {
        return digest.maxTimestampMicros;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(digest);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (other == this)
        {
            return true;
        }
        if (!(other instanceof PartitionUpdateWrapper))
        {
            return false;
        }

        PartitionUpdateWrapper that = (PartitionUpdateWrapper) other;
        return Objects.equals(digest, that.digest);
    }

    @Override
    public int compareTo(@NotNull PartitionUpdateWrapper o)
    {
        return digest.compareTo(o.digest);
    }

    public static class DigestSerializer extends com.esotericsoftware.kryo.Serializer<Digest>
    {

        @Override
        public Digest read(Kryo kryo, Input in, Class type)
        {
            long maxTimestampMicros = in.readLong();
            int size = in.readInt();

            BigInteger token = KryoUtils.readBigInteger(in);

            // read digest
            byte[] digest = in.readBytes(in.readShort());

            String keyspace = in.readString();
            String table = in.readString();

            return new Digest(keyspace, table, maxTimestampMicros, digest, size, token);
        }

        @Override
        public void write(Kryo kryo, Output out, Digest digest)
        {
            out.writeLong(digest.maxTimestampMicros); // 8 bytes
            out.writeInt(digest.dataSize()); // 4 bytes

            KryoUtils.writeBigInteger(out, digest.token); // max 16 bytes

            // write digest
            byte[] ar = digest.array();
            out.writeShort(ar.length);
            out.writeBytes(ar);

            out.writeString(digest.keyspace);
            out.writeString(digest.table);
        }
    }

    /**
     * We use the digest of the PartitionUpdate (by default computed with MD5 in Cassandra
     * Digest.forReadResponse() method) for equality comparisons and to de-duplicate identical mutations
     * across replicas. Using the digest alone significantly reduces the memory footprint of
     * cached mutations that have not achieved the consistency level, and reduces the space used by the
     * CDC state when persisting to durable storage. The tradeoff is that MD5 collisions
     * could potentially result in incorrectly matching updates
     * but we consider the probability to be infinitesimally low as it would require an
     * unintentional MD5 collision within the same token range at around the same write time window; MD5
     * is known to require around ~2^64 operations before achieving a 50% probability of a collision.
     */
    public static class Digest
    {
        public final String keyspace;
        public final String table;
        private final CompletableFuture<byte[]> digest;
        private final long maxTimestampMicros;
        private final CompletableFuture<Integer> dataSize;
        private final BigInteger token;

        public Digest(String keyspace,
                      String table,
                      BigInteger token,
                      int dataSize,
                      @NotNull Supplier<byte[]> digestSupplier,
                      long maxTimestampMicros,
                      @Nullable AsyncExecutor executor)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.maxTimestampMicros = maxTimestampMicros;
            this.token = token;
            int tokenByteLen = RangeUtils.bigIntegerByteArraySize(this.token);
            if (executor != null)
            {
                // use provided executor service to avoid CPU usage on BufferingCommitLogReader i/o thread
                this.digest = executor.submit(digestSupplier);
            }
            else
            {
                this.digest = CompletableFuture.completedFuture(digestSupplier.get());
            }
            this.dataSize = this.digest.thenApply(ar -> 18 /* = 8 + 4 + 2 + 4 */ + tokenByteLen + ar.length + dataSize);
        }

        @VisibleForTesting // and for deserialization
        public Digest(@NotNull String keyspace,
                      @NotNull String table,
                      long maxTimestampMicros,
                      @NotNull byte[] digest,
                      int dataSize,
                      BigInteger token)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.maxTimestampMicros = maxTimestampMicros;
            this.token = token;
            this.digest = CompletableFuture.completedFuture(digest);
            this.dataSize = CompletableFuture.completedFuture(dataSize);
        }

        public Digest(@NotNull String keyspace,
                      @NotNull String table,
                      long maxTimestampMicros,
                      @NotNull CompletableFuture<byte[]> digest,
                      CompletableFuture<Integer> dataSize,
                      BigInteger token)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.maxTimestampMicros = maxTimestampMicros;
            this.token = token;
            this.digest = digest;
            this.dataSize = dataSize;
        }

        public byte[] array()
        {
            return FutureUtils.get(digest);
        }

        public int dataSize()
        {
            return FutureUtils.get(dataSize);
        }

        public String keyspace()
        {
            return keyspace;
        }

        public String table()
        {
            return table;
        }

        public BigInteger token()
        {
            return token;
        }

        public long maxTimestampMicros()
        {
            return maxTimestampMicros;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table);
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null)
            {
                return false;
            }
            if (other == this)
            {
                return true;
            }
            if (!(other instanceof Digest))
            {
                return false;
            }

            Digest that = (Digest) other;
            return Objects.equals(keyspace, that.keyspace)
                   && Objects.equals(table, that.table)
                   && Arrays.equals(array(), that.array());
        }

        public int compareTo(@NotNull Digest o)
        {
            return Long.compare(this.maxTimestampMicros, o.maxTimestampMicros);
        }
    }
}
