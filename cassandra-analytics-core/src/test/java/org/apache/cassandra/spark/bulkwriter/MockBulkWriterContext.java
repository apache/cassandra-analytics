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

package org.apache.cassandra.spark.bulkwriter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;

import o.a.c.sidecar.client.shaded.common.data.TimeSkewResponse;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.common.model.BulkFeatures;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;

public class MockBulkWriterContext implements BulkWriterContext, ClusterInfo, JobInfo, SchemaInfo, DataTransferApi, JobStatsPublisher
{
    private static final long serialVersionUID = -2912371629236770646L;
    public static final String[] DEFAULT_PARTITION_KEY_COLUMNS = {"id", "date"};
    public static final String[] DEFAULT_PRIMARY_KEY_COLUMN_NAMES = {"id", "date"};
    public static final Pair<StructType, ImmutableMap<String, CqlField.CqlType>> DEFAULT_VALID_PAIR =
    TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
    new String[]{"id", "date", "course", "marks"},
    new org.apache.spark.sql.types.DataType[]{DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType, DataTypes.IntegerType},
    new CqlField.CqlType[]{mockCqlType(INT), mockCqlType(DATE), mockCqlType(VARCHAR), mockCqlType(INT)});
    private final boolean quoteIdentifiers;
    private ConsistencyLevel.CL consistencyLevel;
    private int sstableDataSizeInMB = 128;
    private int sstableWriteBatchSize = 2;
    private CassandraBridge bridge = CassandraBridgeFactory.get(CassandraVersion.FOURZERO);

    @Override
    public void publish(Map<String, String> stats)
    {
        // DO NOTHING
    }

    public interface CommitResultSupplier extends BiFunction<List<String>, String, RemoteCommitResult>
    {
    }

    public static final String DEFAULT_CASSANDRA_VERSION = "cassandra-4.0.2";

    private final UUID jobId;
    private Supplier<Long> timeProvider = System::currentTimeMillis;

    private CountDownLatch uploadsLatch = new CountDownLatch(0);
    private boolean skipClean = false;
    public int refreshClusterInfoCallCount = 0;  // CHECKSTYLE IGNORE: Public mutable field
    private final Map<CassandraInstance, List<UploadRequest>> uploads = new ConcurrentHashMap<>();
    private final Map<CassandraInstance, List<String>> commits = new ConcurrentHashMap<>();
    final Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair;
    private final TableSchema schema;
    private final TokenRangeMapping<RingInstance> tokenRangeMapping;
    private final Set<CassandraInstance> cleanCalledForInstance = Collections.synchronizedSet(new HashSet<>());
    private boolean instancesAreAvailable = true;
    private boolean cleanShouldThrow = false;
    private final TokenPartitioner tokenPartitioner;
    private final String cassandraVersion;
    private CommitResultSupplier crSupplier = (uuids, dc) -> new RemoteCommitResult(true, Collections.emptyList(), uuids, null);

    private Predicate<CassandraInstance> uploadRequestConsumer = instance -> true;

    public MockBulkWriterContext(TokenRangeMapping<RingInstance> tokenRangeMapping)
    {
        this(tokenRangeMapping,
             DEFAULT_CASSANDRA_VERSION,
             ConsistencyLevel.CL.LOCAL_QUORUM,
             DEFAULT_VALID_PAIR,
             DEFAULT_PARTITION_KEY_COLUMNS,
             DEFAULT_PRIMARY_KEY_COLUMN_NAMES,
             false);
    }

    public MockBulkWriterContext(TokenRangeMapping<RingInstance> tokenRangeMapping,
                                 String cassandraVersion,
                                 ConsistencyLevel.CL consistencyLevel)
    {
        this(tokenRangeMapping, cassandraVersion, consistencyLevel, DEFAULT_VALID_PAIR, DEFAULT_PARTITION_KEY_COLUMNS, DEFAULT_PRIMARY_KEY_COLUMN_NAMES, false);
    }

    public MockBulkWriterContext(TokenRangeMapping<RingInstance> tokenRangeMapping,
                                 String cassandraVersion,
                                 ConsistencyLevel.CL consistencyLevel,
                                 Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair,
                                 String[] partitionKeyColumns,
                                 String[] primaryKeyColumnNames,
                                 boolean quoteIdentifiers)
    {
        this.quoteIdentifiers = quoteIdentifiers;
        this.tokenRangeMapping = tokenRangeMapping;
        this.tokenPartitioner = new TokenPartitioner(tokenRangeMapping, 1, 2, 2, false);
        this.cassandraVersion = cassandraVersion;
        this.consistencyLevel = consistencyLevel;
        this.validPair = validPair;
        StructType validDataFrameSchema = this.validPair.getKey();
        ImmutableMap<String, CqlField.CqlType> validCqlColumns = this.validPair.getValue();
        ColumnType<?>[] partitionKeyColumnTypes = {ColumnTypes.INT, ColumnTypes.INT};
        TTLOption ttlOption = TTLOption.forever();
        TableSchemaTestCommon.MockTableSchemaBuilder builder = new TableSchemaTestCommon.MockTableSchemaBuilder(CassandraBridgeFactory.get(cassandraVersion))
                                                               .withCqlColumns(validCqlColumns)
                                                               .withPartitionKeyColumns(partitionKeyColumns)
                                                               .withPrimaryKeyColumnNames(primaryKeyColumnNames)
                                                               .withCassandraVersion(cassandraVersion)
                                                               .withPartitionKeyColumnTypes(partitionKeyColumnTypes)
                                                               .withWriteMode(WriteMode.INSERT)
                                                               .withDataFrameSchema(validDataFrameSchema)
                                                               .withTTLSetting(ttlOption);
        if (quoteIdentifiers())
        {
            builder.withQuotedIdentifiers();
        }
        this.schema = builder.build();
        this.jobId = java.util.UUID.randomUUID();
    }

    public Supplier<Long> getTimeProvider()
    {
        return timeProvider;
    }

    public void setTimeProvider(Supplier<Long> timeProvider)
    {
        this.timeProvider = timeProvider;
    }

    public CountDownLatch getUploadsLatch()
    {
        return uploadsLatch;
    }

    public void setUploadsLatch(CountDownLatch uploadsLatch)
    {
        this.uploadsLatch = uploadsLatch;
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public TimeSkewResponse getTimeSkew(List<RingInstance> replicas)
    {
        return new TimeSkewResponse(timeProvider.get(), 60);
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        // TODO: Fix me
        throw new UnsupportedOperationException();
    }

    @Override
    public void refreshClusterInfo()
    {
        refreshClusterInfoCallCount++;
    }

    @Override
    public ConsistencyLevel.CL getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel.CL consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public String getLocalDC()
    {
        return "DC1";
    }

    @Override
    public int sstableDataSizeInMiB()
    {
        return sstableDataSizeInMB;
    }

    @VisibleForTesting
    void setSstableDataSizeInMB(int sstableDataSizeInMB)
    {
        this.sstableDataSizeInMB = sstableDataSizeInMB;
    }

    public int getCommitBatchSize()
    {
        return 1;
    }

    @Override
    public boolean skipExtendedVerify()
    {
        return false;
    }

    @Override
    public boolean getSkipClean()
    {
        return skipClean;
    }

    @NotNull
    @Override
    public DigestAlgorithmSupplier digestAlgorithmSupplier()
    {
        return DigestAlgorithms.XXHASH32;
    }

    public void setSkipCleanOnFailures(boolean skipClean)
    {
        this.skipClean = skipClean;
    }

    @Override
    public int getCommitThreadsPerInstance()
    {
        return 1;
    }

    @Override
    public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
    {
        return tokenRangeMapping;
    }

    @Override
    public UUID getId()
    {
        return jobId;
    }

    @Override
    public TokenPartitioner getTokenPartitioner()
    {
        return tokenPartitioner;
    }

    @Override
    public TableSchema getTableSchema()
    {
        return schema;
    }

    @Override
    public Set<String> getUserDefinedTypeStatements()
    {
        return Collections.emptySet();
    }

    @Override
    public Partitioner getPartitioner()
    {
        return Partitioner.Murmur3Partitioner;
    }

    @Override
    public void checkBulkWriterIsEnabledOrThrow()
    {
        throw new RuntimeException(String.format("Aborting Bulk Writer! feature %s is disabled for cluster",
                                                 BulkFeatures.BULK_WRITER));
    }

    @Override
    public String getLowestCassandraVersion()
    {
        return cassandraVersion;
    }

    @Override
    public RemoteCommitResult commitSSTables(CassandraInstance instance, String migrationId, List<String> uuids)
    {
        commits.computeIfAbsent(instance, k -> new ArrayList<>()).add(migrationId);
        return crSupplier.apply(buildCompleteBatchIds(uuids), instance.datacenter());
    }

    private List<String> buildCompleteBatchIds(List<String> uuids)
    {
        return uuids.stream().map(uuid -> uuid + "-" + jobId).collect(Collectors.toList());
    }

    @Override
    public void cleanUploadSession(CassandraInstance instance, String sessionID, String jobID) throws ClientException
    {
        cleanCalledForInstance.add(instance);
        if (cleanShouldThrow)
        {
            throw new ClientException("Clean was called but was set to throw");
        }
    }

    @Override
    public void uploadSSTableComponent(Path componentFile,
                                       int ssTableIdx,
                                       CassandraInstance instance,
                                       String sessionID,
                                       Digest digest) throws ClientException
    {
        boolean uploadSucceeded = uploadRequestConsumer.test(instance);
        uploads.compute(instance, (k, pathList) -> {
            if (pathList == null)
            {
                pathList = new ArrayList<>();
            }
            pathList.add(new UploadRequest(componentFile, ssTableIdx, instance, sessionID, digest, uploadSucceeded));
            return pathList;
        });
        if (!uploadSucceeded)
        {
            throw new ClientException("Failed upload");
        }
        uploadsLatch.countDown();
    }

    @Override
    public Map<RingInstance, InstanceAvailability> getInstanceAvailability()
    {
        return tokenRangeMapping.getReplicaMetadata().stream()
                                .map(RingInstance::new)
                                .collect(Collectors.toMap(Function.identity(), v -> InstanceAvailability.AVAILABLE));
    }

    @Override
    public boolean instanceIsAvailable(RingInstance ringInstance)
    {
        return instancesAreAvailable;
    }

    @Override
    public InstanceState getInstanceState(RingInstance ringInstance)
    {
        return InstanceState.NORMAL;
    }

    public void setUploadSupplier(Predicate<CassandraInstance> uploadRequestConsumer)
    {
        this.uploadRequestConsumer = uploadRequestConsumer;
    }

    public void setInstancesAreAvailable(boolean instancesAreAvailable)
    {
        this.instancesAreAvailable = instancesAreAvailable;
    }

    public int refreshClusterInfoCallCount()
    {
        return refreshClusterInfoCallCount;
    }

    public List<CassandraInstance> getCleanedInstances()
    {
        return new ArrayList<>(cleanCalledForInstance);
    }

    public void setCleanShouldThrow(boolean cleanShouldThrow)
    {
        this.cleanShouldThrow = cleanShouldThrow;
    }

    public Map<CassandraInstance, List<UploadRequest>> getUploads()
    {
        return uploads;
    }

    public CommitResultSupplier setCommitResultSupplier(CommitResultSupplier supplier)
    {
        CommitResultSupplier oldSupplier = crSupplier;
        crSupplier = supplier;
        return oldSupplier;
    }

    @Override
    public ClusterInfo cluster()
    {
        return this;
    }

    @Override
    public JobInfo job()
    {
        return this;
    }

    @Override
    public JobStatsPublisher jobStats()
    {
        return this;
    }

    @Override
    public SchemaInfo schema()
    {
        return this;
    }

    @Override
    public DataTransferApi transfer()
    {
        return this;
    }

    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public boolean quoteIdentifiers()
    {
        return quoteIdentifiers;
    }

    @Override
    public String keyspace()
    {
        return "keyspace";
    }

    @Override
    public String tableName()
    {
        return "table";
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        StartupValidator.instance().perform();
    }
}
