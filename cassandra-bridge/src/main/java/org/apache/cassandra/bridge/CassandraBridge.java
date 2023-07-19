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
import java.io.File;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.IPartitionUpdateWrapper;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides an abstract interface for all calls to the Cassandra code of a specific version
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public abstract class CassandraBridge
{
    // Used to indicate if a column is unset; used in generating mutations for CommitLog
    @VisibleForTesting
    public static final Object UNSET_MARKER = new Object();

    public static final Pattern COLLECTION_PATTERN = Pattern.compile("^(set|list|map|tuple)<(.+)>$", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROZEN_PATTERN = Pattern.compile("^frozen<(.*)>$", Pattern.CASE_INSENSITIVE);

    public abstract AbstractMap.SimpleEntry<ByteBuffer, BigInteger> getPartitionKey(@NotNull CqlTable table,
                                                                                    @NotNull Partitioner partitioner,
                                                                                    @NotNull List<String> keys);

    public abstract TimeProvider timeProvider();

    // CDC Stream Scanner
    // CHECKSTYLE IGNORE: Method with many parameters
    public abstract StreamScanner<Rid> getCdcScanner(int partitionId,
                                                     @NotNull CqlTable table,
                                                     @NotNull Partitioner partitioner,
                                                     @NotNull CommitLogProvider commitLogProvider,
                                                     @NotNull TableIdLookup tableIdLookup,
                                                     @NotNull Stats stats,
                                                     @Nullable SparkRangeFilter sparkRangeFilter,
                                                     @Nullable CdcOffsetFilter offset,
                                                     int minimumReplicasPerMutation,
                                                     @NotNull Watermarker watermarker,
                                                     @NotNull String jobId,
                                                     @NotNull ExecutorService executorService,
                                                     @NotNull TimeProvider timeProvider);

    // Compaction Stream Scanner
    // CHECKSTYLE IGNORE: Method with many parameters
    public abstract StreamScanner<Rid> getCompactionScanner(@NotNull CqlTable table,
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
        return buildSchema(createStatement, keyspace, replicationFactor, partitioner, udts, null, 0);
    }

    public abstract CqlTable buildSchema(String createStatement,
                                         String keyspace,
                                         ReplicationFactor replicationFactor,
                                         Partitioner partitioner,
                                         Set<String> udts,
                                         @Nullable UUID tableId,
                                         int indexCount);

    // CQL Type Parsing

    public abstract CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input);

    public List<CqlField.NativeType> allTypes()
    {
        return Arrays.asList(ascii(), bigint(), blob(), bool(), counter(), date(), decimal(), aDouble(),
                             duration(), empty(), aFloat(), inet(), aInt(), smallint(), text(), time(),
                             timestamp(), timeuuid(), tinyint(), uuid(), varchar(), varint());
    }

    public abstract Map<String, ? extends CqlField.NativeType> nativeTypeNames();

    public CqlField.NativeType nativeType(String name)
    {
        return nativeTypeNames().get(name.toLowerCase());
    }

    public List<CqlField.NativeType> supportedTypes()
    {
        return allTypes().stream().filter(CqlField.NativeType::isSupported).collect(Collectors.toList());
    }

    // Native

    public abstract CqlField.NativeType ascii();

    public abstract CqlField.NativeType blob();

    public abstract CqlField.NativeType bool();

    public abstract CqlField.NativeType counter();

    public abstract CqlField.NativeType bigint();

    public abstract CqlField.NativeType date();

    public abstract CqlField.NativeType decimal();

    public abstract CqlField.NativeType aDouble();

    public abstract CqlField.NativeType duration();

    public abstract CqlField.NativeType empty();

    public abstract CqlField.NativeType aFloat();

    public abstract CqlField.NativeType inet();

    public abstract CqlField.NativeType aInt();

    public abstract CqlField.NativeType smallint();

    public abstract CqlField.NativeType text();

    public abstract CqlField.NativeType time();

    public abstract CqlField.NativeType timestamp();

    public abstract CqlField.NativeType timeuuid();

    public abstract CqlField.NativeType tinyint();

    public abstract CqlField.NativeType uuid();

    public abstract CqlField.NativeType varchar();

    public abstract CqlField.NativeType varint();

    // Complex

    public abstract CqlField.CqlType collection(String name, CqlField.CqlType... types);

    public abstract CqlField.CqlList list(CqlField.CqlType type);

    public abstract CqlField.CqlSet set(CqlField.CqlType type);

    public abstract CqlField.CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType);

    public abstract CqlField.CqlTuple tuple(CqlField.CqlType... types);

    public abstract CqlField.CqlType frozen(CqlField.CqlType type);

    public abstract CqlField.CqlUdtBuilder udt(String keyspace, String name);

    public CqlField.CqlType parseType(String type)
    {
        return parseType(type, Collections.emptyMap());
    }

    public CqlField.CqlType parseType(String type, Map<String, CqlField.CqlUdt> udts)
    {
        if (type == null || type.length() == 0)
        {
            return null;
        }
        Matcher collectionMatcher = COLLECTION_PATTERN.matcher(type);
        if (collectionMatcher.find())
        {
            // CQL collection
            String[] types = splitInnerTypes(collectionMatcher.group(2));
            return collection(collectionMatcher.group(1), Stream.of(types)
                                                                .map(collectionType -> parseType(collectionType, udts))
                                                                .toArray(CqlField.CqlType[]::new));
        }
        Matcher frozenMatcher = FROZEN_PATTERN.matcher(type);
        if (frozenMatcher.find())
        {
            // Frozen collections
            return frozen(parseType(frozenMatcher.group(1), udts));
        }

        if (udts.containsKey(type))
        {
            // User-defined type
            return udts.get(type);
        }

        // Native CQL 3 type
        return nativeType(type);
    }

    @VisibleForTesting
    public static String[] splitInnerTypes(String str)
    {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parentheses = 0;
        for (int index = 0; index < str.length(); index++)
        {
            char character = str.charAt(index);
            switch (character)
            {
                case ' ':
                    if (parentheses == 0)
                    {
                        continue;
                    }
                    break;
                case ',':
                    if (parentheses == 0)
                    {
                        if (current.length() > 0)
                        {
                            result.add(current.toString());
                            current = new StringBuilder();
                        }
                        continue;
                    }
                    break;
                case '<':
                    parentheses++;
                    break;
                case '>':
                    parentheses--;
                    break;
                default:
                    // Do nothing
            }
            current.append(character);
        }

        if (current.length() > 0 || result.isEmpty())
        {
            result.add(current.toString());
        }

        return result.toArray(new String[0]);
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
                                                   boolean isSorted,
                                                   int bufferSizeMB);

    // CDC Configuration

    public abstract void setCDC(Path path);

    public abstract void setCommitLogPath(Path path);

    @VisibleForTesting
    public abstract ICommitLog testCommitLog(File folder);

    // CommitLog

    public interface IMutation
    {
    }

    public interface IRow
    {
        Object get(int position);

        /**
         * @return true if the entire row is deleted, false otherwise
         */
        default boolean isDeleted()
        {
            return false;
        }

        /**
         * @return true if the row is from an INSERT statement, false otherwise
         */
        default boolean isInsert()
        {
            return true;
        }

        /**
         * Get the range tombstones for this partition
         *
         * TODO: IRow is used as a partition; semantically, it does not fit
         *
         * @return null if no range tombstones exist. Otherwise, return a list of range tombstones
         */
        default List<RangeTombstone> rangeTombstones()
        {
            return null;
        }
    }

    public interface ICommitLog
    {
        void start();

        void stop();

        void clear();

        void add(IMutation mutation);

        void sync();
    }

    /**
     * Cassandra-version-specific implementation for logging a row mutation to CommitLog.
     * Used for CDC unit test framework.`
     *
     * @param table     CQL table schema
     * @param log       CommitLog instance
     * @param row       row instance
     * @param timestamp mutation timestamp
     */
    @VisibleForTesting
    public abstract void log(CqlTable table, ICommitLog log, IRow row, long timestamp);

    /**
     * Determine whether a row is a partition deletion.
     * It is a partition deletion, when all fields except the partition keys are null.
     *
     * @param table CQL table schema
     * @param row   row instance
     * @return true if it is a partition deletion
     */
    protected abstract boolean isPartitionDeletion(CqlTable table, IRow row);

    /**
     * Determine whether a row is a row deletion.
     * It is a row deletion, when all fields except the parimary keys are null.
     *
     * @param table CQL table schema
     * @param row   row instance
     * @return true if it is a row deletion
     */
    protected abstract boolean isRowDeletion(CqlTable table, IRow row);

    @VisibleForTesting
    public abstract Object livingCollectionElement(ByteBuffer cellPath, Object value);

    @VisibleForTesting
    public abstract Object deletedCollectionElement(ByteBuffer cellPath);

    @VisibleForTesting
    public abstract Set<Long> readLog(CqlTable table, CommitLog log, Watermarker watermarker);

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

    public abstract ByteBuffer compress(byte[] bytes) throws IOException;

    public abstract ByteBuffer compress(ByteBuffer input) throws IOException;

    public abstract ByteBuffer uncompress(byte[] bytes) throws IOException;

    public abstract ByteBuffer uncompress(ByteBuffer input) throws IOException;

    // Kryo Serializers

    public abstract Serializer<? extends IPartitionUpdateWrapper> getPartitionUpdateSerializer(
            String keyspace,
            String table,
            boolean includePartitionUpdate);

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
