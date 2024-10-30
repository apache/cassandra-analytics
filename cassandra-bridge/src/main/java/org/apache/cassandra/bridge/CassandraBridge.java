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

package org.apache.cassandra.bridge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.util.CompressionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides an abstract interface for all calls to the Cassandra code of a specific version
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public abstract class CassandraBridge
{
    // Implementations of CassandraBridge must be named as such to load dynamically using the {@link CassandraBridgeFactory}
    public static final String IMPLEMENTATION_FQCN = "org.apache.cassandra.bridge.CassandraBridgeImplementation";

    public abstract CassandraTypes cassandraTypes();

    public abstract AbstractMap.SimpleEntry<ByteBuffer, BigInteger> getPartitionKey(@NotNull CqlTable table,
                                                                                    @NotNull Partitioner partitioner,
                                                                                    @NotNull List<String> keys);

    // Compaction Stream Scanner
    // CHECKSTYLE IGNORE: Method with many parameters
    public abstract StreamScanner<RowData> getCompactionScanner(@NotNull CqlTable table,
                                                                @NotNull Partitioner partitionerType,
                                                                @NotNull SSTablesSupplier ssTables,
                                                                @Nullable SparkRangeFilter sparkRangeFilter,
                                                                @NotNull Collection<PartitionKeyFilter> partitionKeyFilters,
                                                                @Nullable PruneColumnFilter columnFilter,
                                                                @NotNull TimeProvider timeProvider,
                                                                boolean readIndexOffset,
                                                                boolean useIncrementalRepair,
                                                                @NotNull Stats stats);

    public abstract StreamScanner<IndexEntry> getPartitionSizeIterator(@NotNull CqlTable table,
                                                                       @NotNull Partitioner partitioner,
                                                                       @NotNull SSTablesSupplier ssTables,
                                                                       @Nullable SparkRangeFilter rangeFilter,
                                                                       @NotNull TimeProvider timeProvider,
                                                                       @NotNull Stats stats,
                                                                       @NotNull ExecutorService executor);

    public abstract CassandraVersion getVersion();

    public abstract BigInteger hash(Partitioner partitioner, ByteBuffer key);

    public abstract UUID getTimeUUID();

    // CQL Schema

    @VisibleForTesting
    public CqlTable buildSchema(String createStatement, String keyspace)
    {
        return buildSchema(createStatement,
                           keyspace,
                           new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                 ImmutableMap.of("DC1", 3)));
    }

    @VisibleForTesting
    public CqlTable buildSchema(String createStatement, String keyspace, ReplicationFactor replicationFactor)
    {
        return buildSchema(createStatement, keyspace, replicationFactor, Partitioner.Murmur3Partitioner);
    }

    @VisibleForTesting
    public CqlTable buildSchema(String createStatement,
                                String keyspace,
                                ReplicationFactor replicationFactor,
                                Partitioner partitioner)
    {
        return buildSchema(createStatement, keyspace, replicationFactor, partitioner, Collections.emptySet());
    }

    @VisibleForTesting
    public CqlTable buildSchema(String createStatement,
                                String keyspace,
                                ReplicationFactor replicationFactor,
                                Partitioner partitioner,
                                Set<String> udts)
    {
        return buildSchema(createStatement, keyspace, replicationFactor, partitioner, udts, null, 0, false);
    }

    public abstract CqlTable buildSchema(String createStatement,
                                         String keyspace,
                                         ReplicationFactor replicationFactor,
                                         Partitioner partitioner,
                                         Set<String> udts,
                                         @Nullable UUID tableId,
                                         int indexCount,
                                         boolean enableCdc);

    /**
     * Returns the quoted identifier, if the {@code identifier} has mixed case or if the {@code identifier}
     * is a reserved word.
     *
     * @param identifier the identifier
     * @return the quoted identifier when the input is mixed case or a reserved word, the original input otherwise
     */
    public String maybeQuoteIdentifier(String identifier)
    {
        return cassandraTypes().maybeQuoteIdentifier(identifier);
    }

    // CQL Type Parsing

    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        return cassandraTypes().readType(type, input);
    }

    public List<CqlField.NativeType> allTypes()
    {
        return cassandraTypes().allTypes();
    }

    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return cassandraTypes().nativeTypeNames();
    }

    public CqlField.NativeType nativeType(String name)
    {
        return nativeTypeNames().get(name.toLowerCase());
    }

    public List<CqlField.NativeType> supportedTypes()
    {
        return allTypes().stream().filter(CqlField.NativeType::isSupported).collect(Collectors.toList());
    }

    // Native

    public CqlField.NativeType ascii()
    {
        return cassandraTypes().ascii();
    }

    public CqlField.NativeType blob()
    {
        return cassandraTypes().blob();
    }

    public CqlField.NativeType bool()
    {
        return cassandraTypes().bool();
    }

    public CqlField.NativeType counter()
    {
        return cassandraTypes().counter();
    }

    public CqlField.NativeType bigint()
    {
        return cassandraTypes().bigint();
    }

    public CqlField.NativeType date()
    {
        return cassandraTypes().date();
    }

    public CqlField.NativeType decimal()
    {
        return cassandraTypes().decimal();
    }

    public CqlField.NativeType aDouble()
    {
        return cassandraTypes().aDouble();
    }

    public CqlField.NativeType duration()
    {
        return cassandraTypes().duration();
    }

    public CqlField.NativeType empty()
    {
        return cassandraTypes().empty();
    }

    public CqlField.NativeType aFloat()
    {
        return cassandraTypes().aFloat();
    }

    public CqlField.NativeType inet()
    {
        return cassandraTypes().inet();
    }

    public CqlField.NativeType aInt()
    {
        return cassandraTypes().aInt();
    }

    public CqlField.NativeType smallint()
    {
        return cassandraTypes().smallint();
    }

    public CqlField.NativeType text()
    {
        return cassandraTypes().text();
    }

    public CqlField.NativeType time()
    {
        return cassandraTypes().time();
    }

    public CqlField.NativeType timestamp()
    {
        return cassandraTypes().timestamp();
    }

    public CqlField.NativeType timeuuid()
    {
        return cassandraTypes().timeuuid();
    }

    public CqlField.NativeType tinyint()
    {
        return cassandraTypes().tinyint();
    }

    public CqlField.NativeType uuid()
    {
        return cassandraTypes().uuid();
    }

    public CqlField.NativeType varchar()
    {
        return cassandraTypes().varchar();
    }

    public CqlField.NativeType varint()
    {
        return cassandraTypes().varint();
    }

    // Complex

    public CqlField.CqlType collection(String name, CqlField.CqlType... types)
    {
        return cassandraTypes().collection(name, types);
    }

    public CqlField.CqlList list(CqlField.CqlType type)
    {
        return cassandraTypes().list(type);
    }

    public CqlField.CqlSet set(CqlField.CqlType type)
    {
        return cassandraTypes().set(type);
    }

    public CqlField.CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        return cassandraTypes().map(keyType, valueType);
    }

    public CqlField.CqlTuple tuple(CqlField.CqlType... types)
    {
        return cassandraTypes().tuple(types);
    }

    public CqlField.CqlType frozen(CqlField.CqlType type)
    {
        return cassandraTypes().frozen(type);
    }

    public CqlField.CqlUdtBuilder udt(String keyspace, String name)
    {
        return cassandraTypes().udt(keyspace, name);
    }

    public CqlField.CqlType parseType(String type)
    {
        return parseType(type, Collections.emptyMap());
    }

    public CqlField.CqlType parseType(String type, Map<String, CqlField.CqlUdt> udts)
    {
        return cassandraTypes().parseType(type, udts);
    }

    // SSTable Writer

    @FunctionalInterface
    public interface Writer
    {
        void write(Object... values);
    }

    public void writeSSTable(Partitioner partitioner,
                             String keyspace,
                             String table,
                             Path directory,
                             String createStatement,
                             String insertStatement,
                             Consumer<Writer> writer)
    {
        writeSSTable(partitioner,
                     keyspace,
                     table,
                     directory,
                     createStatement,
                     insertStatement,
                     null,
                     false,
                     Collections.emptySet(),
                     writer);
    }

    // CHECKSTYLE IGNORE: Method with many parameters
    public abstract void writeSSTable(Partitioner partitioner,
                                      String keyspace,
                                      String table,
                                      Path directory,
                                      String createStatement,
                                      String insertStatement,
                                      String updateStatement,
                                      boolean upsert,
                                      Set<CqlField.CqlUdt> udts,
                                      Consumer<Writer> writer);

    public abstract SSTableWriter getSSTableWriter(String inDirectory,
                                                   String partitioner,
                                                   String createStatement,
                                                   String insertStatement,
                                                   Set<String> userDefinedTypeStatements,
                                                   int bufferSizeMB);

    public abstract SSTableSummary getSSTableSummary(@NotNull String keyspace,
                                                     @NotNull String table,
                                                     @NotNull SSTable ssTable);

    // Version-Specific Test Utility Methods

    @VisibleForTesting
    public abstract void writeTombstoneSSTable(Partitioner partitioner,
                                               Path directory,
                                               String createStatement,
                                               String deleteStatement,
                                               Consumer<Writer> writer);

    @VisibleForTesting
    public abstract void sstableToJson(Path dataDbFile, OutputStream output) throws FileNotFoundException;

    @VisibleForTesting
    public abstract Object toTupleValue(CqlField.CqlTuple type, Object[] values);

    @VisibleForTesting
    public abstract Object toUserTypeValue(CqlField.CqlUdt type, Map<String, Object> values);

    // Compression Utils

    public ByteBuffer compress(byte[] bytes) throws IOException
    {
        return compressionUtil().compress(bytes);
    }

    public ByteBuffer compress(ByteBuffer input) throws IOException
    {
        return compressionUtil().compress(input);
    }

    public ByteBuffer uncompress(byte[] bytes) throws IOException
    {
        return compressionUtil().uncompress(bytes);
    }

    public ByteBuffer uncompress(ByteBuffer input) throws IOException
    {
        return compressionUtil().uncompress(input);
    }

    public abstract CompressionUtil compressionUtil();

    // Kryo/Java (De-)Serialization

    public abstract void kryoRegister(Kryo kryo);

    public abstract void javaSerialize(ObjectOutputStream out, Serializable object);

    public abstract <T> T javaDeserialize(ObjectInputStream in, Class<T> type);

    public byte[] javaSerialize(Serializable object)
    {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
             ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            javaSerialize(out, object);
            return bytes.toByteArray();
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public <T> T javaDeserialize(byte[] bytes, Class<T> type)
    {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes)))
        {
            return javaDeserialize(in, type);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
