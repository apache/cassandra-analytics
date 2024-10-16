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

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.model.BulkFeatures;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.exception.TimeSkewTooLargeException;
import org.apache.cassandra.spark.validation.StartupValidator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;

public class MockBulkWriterContext implements BulkWriterContext, ClusterInfo, JobInfo, SchemaInfo, JobStatsPublisher
{
    private static final long serialVersionUID = -2912371629236770646L;
    public static final String[] DEFAULT_PARTITION_KEY_COLUMNS = {"id", "date"};
    public static final String[] DEFAULT_PRIMARY_KEY_COLUMN_NAMES = {"id", "date"};
    public static final Pair<StructType, ImmutableMap<String, CqlField.CqlType>> DEFAULT_VALID_PAIR =
    TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
    new String[]{"id", "date", "course", "marks"},
    new org.apache.spark.sql.types.DataType[]{DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType, DataTypes.IntegerType},
    new CqlField.CqlType[]{mockCqlType(INT), mockCqlType(DATE), mockCqlType(VARCHAR), mockCqlType(INT)});
    private ConsistencyLevel.CL consistencyLevel;
    private int sstableDataSizeInMB = 128;
    private CassandraBridge bridge = CassandraBridgeFactory.get(CassandraVersion.FOURZERO);
    private TimeSkewTooLargeException timeSkewTooLargeException;

    @Override
    public void publish(Map<String, String> stats)
    {
        // DO NOTHING
    }

    public interface CommitResultSupplier extends BiFunction<List<String>, String, DirectDataTransferApi.RemoteCommitResult>
    {
    }

    public static final String DEFAULT_CASSANDRA_VERSION = "cassandra-4.0.2";

    private final UUID jobId;
    private boolean skipClean = false;
    public int refreshClusterInfoCallCount = 0;  // CHECKSTYLE IGNORE: Public mutable field
    private final Map<CassandraInstance, List<UploadRequest>> uploads = new ConcurrentHashMap<>();
    private final Map<CassandraInstance, List<String>> commits = new ConcurrentHashMap<>();
    final Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair;
    private final TableSchema schema;
    private final TokenRangeMapping<RingInstance> tokenRangeMapping;
    private final Set<CassandraInstance> cleanCalledForInstance = Collections.synchronizedSet(new HashSet<>());
    private boolean cleanShouldThrow = false;
    private final TokenPartitioner tokenPartitioner;
    private final String cassandraVersion;
    private CommitResultSupplier crSupplier = (uuids, dc) -> new DirectDataTransferApi.RemoteCommitResult(true, Collections.emptyList(), uuids, null);
    private Predicate<CassandraInstance> uploadRequestConsumer = instance -> true;
    private ReplicationFactor replicationFactor;

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
        if (quoteIdentifiers)
        {
            builder.withQuotedIdentifiers();
        }
        this.schema = builder.build();
        this.jobId = java.util.UUID.randomUUID();
    }

    @Override
    public void shutdown()
    {
    }

    public void setTimeSkewTooLargeException(TimeSkewTooLargeException exception)
    {
        this.timeSkewTooLargeException = exception;
    }

    @Override
    public void validateTimeSkew(Range<BigInteger> range) throws SidecarApiCallException, TimeSkewTooLargeException
    {
        if (timeSkewTooLargeException != null)
        {
            throw timeSkewTooLargeException;
        }
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        // TODO: Fix me
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
    }

    public void setReplicationFactor(ReplicationFactor replicationFactor)
    {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public CassandraContext getCassandraContext()
    {
        return null;
    }

    @Override
    public String clusterId()
    {
        return "test-cluster";
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

    @Override
    public String getLocalDC()
    {
        if (getConsistencyLevel().isLocal())
        {
            return "DC1";
        }
        return null;
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

    @Override
    public DataTransportInfo transportInfo()
    {
        return new DataTransportInfo(DataTransport.DIRECT, null, 0);
    }

    @Override
    public int jobKeepAliveMinutes()
    {
        return 1;
    }

    @Override
    public long jobTimeoutSeconds()
    {
        return -1;
    }

    @Override
    public int effectiveSidecarPort()
    {
        return 9043;
    }

    @Override
    public double importCoordinatorTimeoutMultiplier()
    {
        return 2.0;
    }

    @Nullable
    @Override
    public CoordinatedWriteConf coordinatedWriteConf()
    {
        return null;
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
    public UUID getRestoreJobId()
    {
        return jobId;
    }

    @Override
    public String getConfiguredJobId()
    {
        return null;
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

    private List<String> buildCompleteBatchIds(List<String> uuids)
    {
        return uuids.stream().map(uuid -> uuid + "-" + jobId).collect(Collectors.toList());
    }

    @Override
    public Map<RingInstance, WriteAvailability> clusterWriteAvailability()
    {
        Set<RingInstance> allInstances = tokenRangeMapping.allInstances();
        Map<RingInstance, WriteAvailability> result = new HashMap<>(allInstances.size());
        for (RingInstance instance : allInstances)
        {
            result.put(instance, WriteAvailability.AVAILABLE);
        }
        return result;
    }

    public void setUploadSupplier(Predicate<CassandraInstance> uploadRequestConsumer)
    {
        this.uploadRequestConsumer = uploadRequestConsumer;
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
    public TransportContext transportContext()
    {
        MockBulkWriterContext mockBulkWriterContext = this;
        return new TransportContext.DirectDataBulkWriterContext()
        {
            @Override
            public DirectDataTransferApi dataTransferApi()
            {
                return new DirectDataTransferApi()
                {
                    @Override
                    public DirectDataTransferApi.RemoteCommitResult commitSSTables(CassandraInstance instance, String migrationId, List<String> uuids)
                    {
                        commits.compute(instance, (ignored, commitList) -> {
                            if (commitList == null)
                            {
                                commitList = new ArrayList<>();
                            }
                            commitList.add(migrationId);
                            return commitList;
                        });
                        return crSupplier.apply(buildCompleteBatchIds(uuids), instance.datacenter());
                    }

                    @Override
                    public void cleanUploadSession(CassandraInstance instance, String sessionID, String jobID) throws SidecarApiCallException
                    {
                        cleanCalledForInstance.add(instance);
                        if (cleanShouldThrow)
                        {
                            throw new SidecarApiCallException("Clean was called but was set to throw");
                        }
                    }

                    @Override
                    public void uploadSSTableComponent(Path componentFile,
                                                       int ssTableIdx,
                                                       CassandraInstance instance,
                                                       String sessionID,
                                                       Digest digest) throws SidecarApiCallException
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
                            throw new SidecarApiCallException("Failed upload");
                        }
                    }
                };
            }

            @Override
            public StreamSession<?> createStreamSession(BulkWriterContext writerContext,
                                                        String sessionId,
                                                        SortedSSTableWriter sstableWriter,
                                                        Range<BigInteger> range,
                                                        ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                                        ExecutorService executorService)
            {
                return new DirectStreamSession(mockBulkWriterContext,
                                               sstableWriter,
                                               this,
                                               sessionId,
                                               range,
                                               failureHandler,
                                               executorService);
            }
        };
    }

    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public QualifiedTableName qualifiedTableName()
    {
        return new QualifiedTableName("keyspace", "table", false);
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        StartupValidator.instance().perform();
    }
}
