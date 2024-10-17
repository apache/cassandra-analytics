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

package org.apache.cassandra.spark.data;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.spark.utils.Throwing;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.parquet.Strings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Basic DataLayer implementation to read SSTables from local file system. Mostly used for testing.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class LocalDataLayer extends DataLayer implements Serializable
{
    public static final long serialVersionUID = 42L;

    private transient CassandraBridge bridge;
    private Partitioner partitioner;
    private CqlTable cqlTable;
    private String jobId;
    private String statsClass;
    private transient volatile Stats stats = null;
    private List<SchemaFeature> requestedFeatures;
    private boolean useBufferingInputStream;
    private String[] paths;
    private int minimumReplicasPerMutation = 1;
    private Set<Path> dataFilePaths = null;

    @Nullable
    private static Stats loadStats(@Nullable String statsClass)
    {
        if (Strings.isNullOrEmpty(statsClass))
        {
            return null;
        }
        // For tests it's useful to inject a custom stats instance to collect & verify metrics
        try
        {
            int index = statsClass.lastIndexOf(".");
            String className = statsClass.substring(0, index);
            String fieldName = statsClass.substring(index + 1);
            Field field = Class.forName(className).getDeclaredField(fieldName);
            return (Stats) field.get(null);
        }
        catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    /**
     * Returns the lower-cased key using {@link Locale#ROOT}
     *
     * @param key the key
     * @return the lower-cased key using {@link Locale#ROOT}
     */
    @Nullable
    private static String lowerCaseKey(@Nullable String key)
    {
        return key != null ? key.toLowerCase(Locale.ROOT) : null;
    }

    @NotNull
    private static String getOrThrow(@NotNull Map<String, String> options, @Nullable String key)
    {
        String value = options.get(key);
        if (value != null)
        {
            return value;
        }
        else
        {
            throw new IllegalArgumentException("Value for key '" + key + "' is missing from the options map");
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static boolean getBoolean(@NotNull Map<String, String> options, @Nullable String key, boolean defaultValue)
    {
        String value = options.get(key);
        // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings
        if (value == null)
        {
            return defaultValue;
        }
        else if (value.equalsIgnoreCase("true"))
        {
            return true;
        }
        else if (value.equalsIgnoreCase("false"))
        {
            return false;
        }
        else
        {
            throw new IllegalArgumentException("Value '" + value + "' for key '" + key + "' is not a boolean string");
        }
    }

    /**
     * Builds a new {@link DataLayer} from the {@code options} map. The keys for the map
     * must be lower-cased to guarantee compatibility with maps where the keys are all
     * lower-cased.
     *
     * @param options the map with options
     * @return a new {@link DataLayer}
     */
    public static LocalDataLayer from(Map<String, String> options)
    {
        // Keys need to be lower-cased to access the map
        return new LocalDataLayer(
                CassandraVersion.valueOf(options.getOrDefault(lowerCaseKey("version"), CassandraVersion.FOURZERO.name())),
                Partitioner.valueOf(options.getOrDefault(lowerCaseKey("partitioner"), Partitioner.Murmur3Partitioner.name())),
                getOrThrow(options, lowerCaseKey("keyspace")),
                getOrThrow(options, lowerCaseKey("createStmt")),
                Arrays.stream(options.getOrDefault(lowerCaseKey("udts"), "").split("\n"))
                      .filter(StringUtils::isNotEmpty)
                      .collect(Collectors.toSet()),
                SchemaFeatureSet.initializeFromOptions(options),
                getBoolean(options, lowerCaseKey("useBufferingInputStream"), getBoolean(options, lowerCaseKey("useSSTableInputStream"), false)),
                options.get(lowerCaseKey("statsClass")),
                getOrThrow(options, lowerCaseKey("dirs")).split(","));
    }

    public LocalDataLayer(@NotNull CassandraVersion version,
                          @NotNull String keyspace,
                          @NotNull String createStatement,
                          String... paths)
    {
        this(version,
             Partitioner.Murmur3Partitioner,
             keyspace,
             createStatement,
             Collections.emptySet(),
             Collections.emptyList(),
             false,
             null,
             paths);
    }

    public LocalDataLayer(@NotNull CassandraVersion version,
                          @NotNull String keyspace,
                          @NotNull String createStatement,
                          @NotNull Set<String> udtStatements,
                          String... paths)
    {
        this(version,
             Partitioner.Murmur3Partitioner,
             keyspace,
             createStatement,
             udtStatements,
             Collections.emptyList(),
             false,
             null,
             paths);
    }

    // CHECKSTYLE IGNORE: Constructor with many parameters
    public LocalDataLayer(@NotNull CassandraVersion version,
                          @NotNull Partitioner partitioner,
                          @NotNull String keyspace,
                          @NotNull String createStatement,
                          @NotNull Set<String> udts,
                          @NotNull List<SchemaFeature> requestedFeatures,
                          boolean useBufferingInputStream,
                          @Nullable String statsClass,
                          String... paths)
    {
        this.bridge = CassandraBridgeFactory.get(version);
        this.partitioner = partitioner;
        this.cqlTable = bridge().buildSchema(createStatement,
                                             keyspace,
                                             new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                   ImmutableMap.of("replication_factor", 1)),
                                             partitioner,
                                             udts);
        this.jobId = UUID.randomUUID().toString();
        this.requestedFeatures = requestedFeatures;
        this.useBufferingInputStream = useBufferingInputStream;
        this.statsClass = statsClass;
        this.paths = paths;
        this.dataFilePaths = new HashSet<>();
    }

    // For serialization
    private LocalDataLayer(@NotNull CassandraVersion version,
                           @NotNull Partitioner partitioner,
                           @NotNull CqlTable cqlTable,
                           @NotNull String jobId,
                           @NotNull List<SchemaFeature> requestedFeatures,
                           boolean useBufferingInputStream,
                           @Nullable String statsClass,
                           String... paths)
    {
        this.bridge = CassandraBridgeFactory.get(version);
        this.partitioner = partitioner;
        this.cqlTable = cqlTable;
        this.jobId = jobId;
        this.requestedFeatures = requestedFeatures;
        this.useBufferingInputStream = useBufferingInputStream;
        this.statsClass = statsClass;
        this.paths = paths;
    }

    @Override
    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    @Override
    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public Partitioner partitioner()
    {
        return partitioner;
    }

    @Override
    public int partitionCount()
    {
        return 1;
    }

    @Override
    public boolean isInPartition(int partitionId, BigInteger token, ByteBuffer key)
    {
        return true;
    }

    @Override
    public TimeProvider timeProvider()
    {
        return TimeProvider.DEFAULT;
    }

    @Override
    public CqlTable cqlTable()
    {
        return cqlTable;
    }

    @Override
    public synchronized Stats stats()
    {
        if (stats == null)
        {
            stats = loadStats(statsClass);
        }
        if (stats == null)
        {
            stats = super.stats();
        }
        return stats;
    }

    public void setDataFilePaths(Set<Path> dataFilePaths)
    {
        this.dataFilePaths = dataFilePaths;
    }

    @Override
    public SSTablesSupplier sstables(int partitionId,
                                     @Nullable SparkRangeFilter sparkRangeFilter,
                                     @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        Stream<Path> dataFilePathsStream;
        // if data file paths is supplied, prefer them over listing files
        if (dataFilePaths != null && !dataFilePaths.isEmpty())
        {
            dataFilePathsStream = dataFilePaths.stream();
        }
        else
        {
            dataFilePathsStream = Arrays
                                  .stream(paths)
                                  .map(Paths::get)
                                  .flatMap(Throwing.function(Files::list))
                                  .filter(path -> path.getFileName().toString().endsWith("-" + FileType.DATA.getFileSuffix()));
        }

        return new BasicSupplier(dataFilePathsStream
                .map(path -> new FileSystemSSTable(path, useBufferingInputStream, () -> this.stats.bufferingInputStreamStats()))
                .collect(Collectors.toSet()));
    }

    @VisibleForTesting
    public LocalDataLayer withMinimumReplicasPerMutation(int minimumReplicasPerMutation)
    {
        this.minimumReplicasPerMutation = minimumReplicasPerMutation;
        return this;
    }

    public String jobId()
    {
        return jobId;
    }

    @Override
    protected ExecutorService executorService()
    {
        return FileSystemSource.FILE_IO_EXECUTOR;
    }

    private static Stream<Path> listPath(Path path)
    {
        try
        {
            return Files.list(path);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(cqlTable)
               .append(paths)
               .append(version())
               .toHashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        LocalDataLayer that = (LocalDataLayer) other;
        return new EqualsBuilder()
               .append(this.cqlTable, that.cqlTable)
               .append(this.paths, that.paths)
               .append(this.version(), that.version())
               .isEquals();
    }

    // JDK Serialization

    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        // Falling back to JDK serialization
        out.writeUTF(this.version().name());
        out.writeObject(this.partitioner);
        bridge.javaSerialize(out, this.cqlTable);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        out.writeUTF(this.jobId);
        out.writeObject(this.requestedFeatures.stream()
                                              .map(SchemaFeature::toString)
                                              .toArray(String[]::new));
        out.writeBoolean(this.useBufferingInputStream);
        out.writeBoolean(this.statsClass != null);
        if (this.statsClass != null)
        {
            out.writeUTF(this.statsClass);
        }
        out.writeObject(this.paths);
        out.writeInt(this.minimumReplicasPerMutation);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        // Falling back to JDK deserialization
        this.bridge = CassandraBridgeFactory.get(CassandraVersion.valueOf(in.readUTF()));
        this.partitioner = (Partitioner) in.readObject();
        this.cqlTable = bridge.javaDeserialize(in, CqlTable.class);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
        this.jobId = in.readUTF();
        this.requestedFeatures = Arrays.stream((String[]) in.readObject())
                                       .map(SchemaFeatureSet::valueOf)
                                       .collect(Collectors.toList());
        this.useBufferingInputStream = in.readBoolean();
        this.statsClass = in.readBoolean() ? in.readUTF() : null;
        this.paths = (String[]) in.readObject();
        this.minimumReplicasPerMutation = in.readInt();
    }

    // Kryo Serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<LocalDataLayer>
    {
        @Override
        public void write(Kryo kryo, Output out, LocalDataLayer object)
        {
            kryo.writeObject(out, object.version());
            kryo.writeObject(out, object.partitioner);
            kryo.writeObject(out, object.cqlTable);
            out.writeString(object.jobId);
            kryo.writeObject(out, object.requestedFeatures.stream()
                                                          .map(SchemaFeature::toString)
                                                          .toArray(String[]::new));
            out.writeBoolean(object.useBufferingInputStream);
            out.writeString(object.statsClass);
            kryo.writeObject(out, object.paths);
            out.writeInt(object.minimumReplicasPerMutation);
        }

        @Override
        public LocalDataLayer read(Kryo kryo, Input in, Class<LocalDataLayer> type)
        {
            return new LocalDataLayer(
                    kryo.readObject(in, CassandraVersion.class),
                    kryo.readObject(in, Partitioner.class),
                    kryo.readObject(in, CqlTable.class),
                    in.readString(),
                    Arrays.stream(kryo.readObject(in, String[].class))
                          .map(SchemaFeatureSet::valueOf)
                          .collect(Collectors.toList()),
                    in.readBoolean(),
                    in.readString(),
                    kryo.readObject(in, String[].class)
            ).withMinimumReplicasPerMutation(in.readInt());
        }
    }
}
