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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.common.MD5Hash;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.common.model.BulkFeatures;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;

public class MockBulkWriterContext implements BulkWriterContext, ClusterInfo, JobInfo, SchemaInfo, DataTransferApi
{
    private static final long serialVersionUID = -2912371629236770646L;
    private RowBufferMode rowBufferMode = RowBufferMode.UNBUFFERRED;
    private ConsistencyLevel.CL consistencyLevel;

    public interface CommitResultSupplier extends BiFunction<List<String>, String, RemoteCommitResult>
    {
    }

    public static final String DEFAULT_CASSANDRA_VERSION = "cassandra-4.0.2";

    private final UUID jobId;
    private Supplier<Long> timeProvider = System::currentTimeMillis;
    private boolean skipClean = false;
    public int refreshClusterInfoCallCount = 0;  // CHECKSTYLE IGNORE: Public mutable field
    private final Map<CassandraInstance, List<UploadRequest>> uploads = new HashMap<>();
    private final Map<CassandraInstance, List<String>> commits = new HashMap<>();
    final Pair<StructType, ImmutableMap<String, CqlField.CqlType>> validPair;
    private final TableSchema schema;
    private final Set<CassandraInstance> cleanCalledForInstance = new HashSet<>();
    private boolean instancesAreAvailable = true;
    private boolean cleanShouldThrow = false;
    private final CassandraRing<RingInstance> ring;
    private final TokenPartitioner tokenPartitioner;
    private final String cassandraVersion;
    private CommitResultSupplier crSupplier = (uuids, dc) -> new RemoteCommitResult(true, Collections.emptyList(), uuids, null);

    private Predicate<CassandraInstance> uploadRequestConsumer = instance -> true;

    public MockBulkWriterContext(CassandraRing<RingInstance> ring,
                                 String cassandraVersion,
                                 ConsistencyLevel.CL consistencyLevel)
    {
        this.ring = ring;
        this.tokenPartitioner = new TokenPartitioner(ring, 1, 2, 2, false);
        this.cassandraVersion = cassandraVersion;
        this.consistencyLevel = consistencyLevel;
        validPair = TableSchemaTestCommon.buildMatchedDataframeAndCqlColumns(
                new String[]{"id", "date", "course", "marks"},
                new org.apache.spark.sql.types.DataType[]{DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType, DataTypes.IntegerType},
                new CqlField.CqlType[]{mockCqlType(INT), mockCqlType(DATE), mockCqlType(VARCHAR), mockCqlType(INT)});
        StructType validDataFrameSchema = validPair.getKey();
        ImmutableMap<String, CqlField.CqlType> validCqlColumns = validPair.getValue();
        String[] partitionKeyColumns = {"id", "date"};
        String[] primaryKeyColumnNames = {"id", "date"};
        ColumnType<?>[] partitionKeyColumnTypes = {ColumnTypes.INT, ColumnTypes.INT};
        this.schema = new TableSchemaTestCommon.MockTableSchemaBuilder()
                      .withCqlColumns(validCqlColumns)
                      .withPartitionKeyColumns(partitionKeyColumns)
                      .withPrimaryKeyColumnNames(primaryKeyColumnNames)
                      .withCassandraVersion(cassandraVersion)
                      .withPartitionKeyColumnTypes(partitionKeyColumnTypes)
                      .withWriteMode(WriteMode.INSERT)
                      .withDataFrameSchema(validDataFrameSchema)
                      .build();
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

    public MockBulkWriterContext(CassandraRing<RingInstance> ring)
    {
        this(ring, DEFAULT_CASSANDRA_VERSION, ConsistencyLevel.CL.LOCAL_QUORUM);
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
    public RowBufferMode getRowBufferMode()
    {
        return rowBufferMode;
    }

    public void setRowBufferMode(RowBufferMode rowBufferMode)
    {
        this.rowBufferMode = rowBufferMode;
    }

    @Override
    public int getSstableDataSizeInMB()
    {
        return 128;
    }

    @Override
    public int getSstableBatchSize()
    {
        return 1;
    }

    @Override
    public int getCommitBatchSize()
    {
        return 1;
    }

    @Override
    public boolean validateSSTables()
    {
        return true;
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
    public CassandraRing<RingInstance> getRing(boolean cached)
    {
        return ring;
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
        commits.compute(instance, (ignored, commitList) -> {
            if (commitList == null)
            {
                commitList = new ArrayList<>();
            }
            commitList.add(migrationId);
            return commitList;
        });
        return crSupplier.apply(buildCompleteBatchIds(uuids), instance.getDataCenter());
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
                                       MD5Hash fileHash) throws ClientException
    {
        boolean uploadSucceeded = uploadRequestConsumer.test(instance);
        uploads.compute(instance, (k, pathList) -> {
            if (pathList == null)
            {
                pathList = new ArrayList<>();
            }
            pathList.add(new UploadRequest(componentFile, ssTableIdx, instance, sessionID, fileHash, uploadSucceeded));
            return pathList;
        });
        if (!uploadSucceeded)
        {
            throw new ClientException("Failed upload");
        }
    }

    @Override
    public Map<RingInstance, InstanceAvailability> getInstanceAvailability()
    {
        return null;
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
    public SchemaInfo schema()
    {
        return this;
    }

    @Override
    public DataTransferApi transfer()
    {
        return this;
    }

    @Override
    public String getFullTableName()
    {
        return "keyspace.table";
    }
}
