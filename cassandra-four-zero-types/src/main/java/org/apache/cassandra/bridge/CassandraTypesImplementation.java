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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.complex.CqlCollection;
import org.apache.cassandra.spark.data.complex.CqlFrozen;
import org.apache.cassandra.spark.data.complex.CqlUdt;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.types.Ascii;
import org.apache.cassandra.spark.data.types.BigInt;
import org.apache.cassandra.spark.data.types.Blob;
import org.apache.cassandra.spark.data.types.Boolean;
import org.apache.cassandra.spark.data.types.Counter;
import org.apache.cassandra.spark.data.types.Date;
import org.apache.cassandra.spark.data.types.Decimal;
import org.apache.cassandra.spark.data.types.Double;
import org.apache.cassandra.spark.data.types.Duration;
import org.apache.cassandra.spark.data.types.Empty;
import org.apache.cassandra.spark.data.types.Float;
import org.apache.cassandra.spark.data.types.Inet;
import org.apache.cassandra.spark.data.types.Int;
import org.apache.cassandra.spark.data.types.SmallInt;
import org.apache.cassandra.spark.data.types.Text;
import org.apache.cassandra.spark.data.types.Time;
import org.apache.cassandra.spark.data.types.TimeUUID;
import org.apache.cassandra.spark.data.types.Timestamp;
import org.apache.cassandra.spark.data.types.TinyInt;
import org.apache.cassandra.spark.data.types.VarChar;
import org.apache.cassandra.spark.data.types.VarInt;

public class CassandraTypesImplementation extends CassandraTypes
{
    private static volatile boolean setup = false;

    public static final CassandraTypesImplementation INSTANCE = new CassandraTypesImplementation();

    public static synchronized void setup()
    {
        if (!CassandraTypesImplementation.setup)
        {
            // We never want to enable mbean registration in the Cassandra code we use so disable it here
            System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
            Config.setClientMode(true);
            // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
            // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
            // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            DatabaseDescriptor.clientInitialization();
            Config config = DatabaseDescriptor.getRawConfig();
            config.memtable_flush_writers = 8;
            config.diagnostic_events_enabled = false;
            config.max_mutation_size_in_kb = config.commitlog_segment_size_in_mb * 1024 / 2;
            config.concurrent_compactors = 4;
            Path tempDirectory;
            try
            {
                tempDirectory = Files.createTempDirectory(UUID.randomUUID().toString());
            }
            catch (IOException exception)
            {
                throw new RuntimeException(exception);
            }
            config.data_file_directories = new String[]{tempDirectory.toString()};
            setupCommitLogConfigs(tempDirectory);
            DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
            Keyspace.setInitialized();
            setup = true;
        }
    }

    protected static void setupCommitLogConfigs(Path path)
    {
        Path commitLogPath = path.resolve("commitlog");
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath.toString();
        DatabaseDescriptor.getRawConfig().hints_directory = path.resolve("hints").toString();
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path.resolve("saved_caches").toString();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(32);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(commitLog -> new CommitLogSegmentManagerStandard(commitLog, commitLogPath.toString()));
    }

    private final Map<String, CqlField.NativeType> nativeTypes;

    public CassandraTypesImplementation()
    {
        nativeTypes = allTypes().stream().collect(Collectors.toMap(CqlField.CqlType::name, Function.identity()));
    }

    public static IPartitioner getPartitioner(Partitioner partitioner)
    {
        return partitioner == Partitioner.Murmur3Partitioner ? Murmur3Partitioner.instance : RandomPartitioner.instance;
    }

    @Override
    public String maybeQuoteIdentifier(String identifier)
    {
        if (isAlreadyQuoted(identifier))
        {
            return identifier;
        }
        return ColumnIdentifier.maybeQuote(identifier);
    }

    @Override
    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return nativeTypes;
    }

    @Override
    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        switch (type)
        {
            case NativeCql:
                return nativeType(input.readString());
            case Set:
            case List:
            case Map:
            case Tuple:
                return CqlCollection.read(type, input, this);
            case Frozen:
                return CqlFrozen.build(CqlField.CqlType.read(input, this));
            case Udt:
                return CqlUdt.read(input, this);
            default:
                throw new IllegalStateException("Unknown CQL type, cannot deserialize");
        }
    }

    @Override
    public Ascii ascii()
    {
        return Ascii.INSTANCE;
    }

    @Override
    public Blob blob()
    {
        return Blob.INSTANCE;
    }

    @Override
    public Boolean bool()
    {
        return Boolean.INSTANCE;
    }

    @Override
    public Counter counter()
    {
        return Counter.INSTANCE;
    }

    @Override
    public BigInt bigint()
    {
        return BigInt.INSTANCE;
    }

    @Override
    public Date date()
    {
        return Date.INSTANCE;
    }

    @Override
    public Decimal decimal()
    {
        return Decimal.INSTANCE;
    }

    @Override
    public Double aDouble()
    {
        return Double.INSTANCE;
    }

    @Override
    public Duration duration()
    {
        return Duration.INSTANCE;
    }

    @Override
    public Empty empty()
    {
        return Empty.INSTANCE;
    }

    @Override
    public Float aFloat()
    {
        return Float.INSTANCE;
    }

    @Override
    public Inet inet()
    {
        return Inet.INSTANCE;
    }

    @Override
    public Int aInt()
    {
        return Int.INSTANCE;
    }

    @Override
    public SmallInt smallint()
    {
        return SmallInt.INSTANCE;
    }

    @Override
    public Text text()
    {
        return Text.INSTANCE;
    }

    @Override
    public Time time()
    {
        return Time.INSTANCE;
    }

    @Override
    public Timestamp timestamp()
    {
        return Timestamp.INSTANCE;
    }

    @Override
    public TimeUUID timeuuid()
    {
        return TimeUUID.INSTANCE;
    }

    @Override
    public TinyInt tinyint()
    {
        return TinyInt.INSTANCE;
    }

    @Override
    public org.apache.cassandra.spark.data.types.UUID uuid()
    {
        return org.apache.cassandra.spark.data.types.UUID.INSTANCE;
    }

    @Override
    public VarChar varchar()
    {
        return VarChar.INSTANCE;
    }

    @Override
    public VarInt varint()
    {
        return VarInt.INSTANCE;
    }

    @Override
    public CqlField.CqlType collection(String name, CqlField.CqlType... types)
    {
        return CqlCollection.build(name, types);
    }

    @Override
    public CqlField.CqlList list(CqlField.CqlType type)
    {
        return CqlCollection.list(type);
    }

    @Override
    public CqlField.CqlSet set(CqlField.CqlType type)
    {
        return CqlCollection.set(type);
    }

    @Override
    public CqlField.CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        return CqlCollection.map(keyType, valueType);
    }

    @Override
    public CqlField.CqlTuple tuple(CqlField.CqlType... types)
    {
        return CqlCollection.tuple(types);
    }

    @Override
    public CqlField.CqlType frozen(CqlField.CqlType type)
    {
        return CqlFrozen.build(type);
    }

    @Override
    public CqlField.CqlUdtBuilder udt(String keyspace, String name)
    {
        return CqlUdt.builder(keyspace, name);
    }
}
