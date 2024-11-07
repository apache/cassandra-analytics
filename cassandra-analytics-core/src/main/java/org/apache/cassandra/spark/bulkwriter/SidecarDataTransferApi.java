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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.request.ImportSSTableRequest;
import o.a.c.sidecar.client.shaded.common.response.SSTableImportResponse;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.exception.SidecarApiCallException;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.maybeQuotedIdentifier;

/**
 * A {@link DirectDataTransferApi} implementation that interacts with Cassandra Sidecar
 */
public class SidecarDataTransferApi implements DirectDataTransferApi
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarDataTransferApi.class);
    private static final String SSTABLE_NAME_SEPARATOR = "-";
    private static final int SSTABLE_GENERATION_REVERSE_OFFSET = 3;

    private final SidecarClient sidecarClient;
    private final CassandraBridge bridge;
    private final int sidecarPort;
    private final JobInfo job;

    public SidecarDataTransferApi(CassandraContext cassandraContext, CassandraBridge bridge, JobInfo job)
    {
        this.sidecarClient = cassandraContext.getSidecarClient();
        this.sidecarPort = cassandraContext.sidecarPort();
        this.bridge = bridge;
        this.job = job;
    }

    @Override
    public void uploadSSTableComponent(Path componentFile,
                                       int ssTableIdx,
                                       CassandraInstance instance,
                                       String sessionID,
                                       Digest digest) throws SidecarApiCallException
    {
        String componentName = updateComponentName(componentFile, ssTableIdx);
        String uploadId = getUploadId(sessionID, job.getRestoreJobId().toString());
        QualifiedTableName qt = job.qualifiedTableName();
        try
        {
            sidecarClient.uploadSSTableRequest(toSidecarInstance(instance),
                                               maybeQuotedIdentifier(bridge, qt.quoteIdentifiers(), qt.keyspace()),
                                               maybeQuotedIdentifier(bridge, qt.quoteIdentifiers(), qt.table()),
                                               uploadId,
                                               componentName,
                                               digest.toSidecarDigest(),
                                               componentFile.toAbsolutePath().toString())
                         .get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.warn("Failed to upload file={}, keyspace={}, table={}, uploadId={}, componentName={}, instance={}",
                        componentFile, qt.keyspace(), qt.table(), uploadId, componentName, instance, exception);
            throw new SidecarApiCallException(
            String.format("Failed to upload file=%s into keyspace=%s, table=%s, componentName=%s with uploadId=%s to instance=%s. Cause=%s",
                          componentFile, qt.keyspace(), qt.table(), componentName, uploadId, instance, exception.getMessage()), exception);
        }
    }

    @Override
    public RemoteCommitResult commitSSTables(CassandraInstance instance,
                                             String migrationId,
                                             List<String> uuids) throws SidecarApiCallException
    {
        if (uuids.size() != 1)
        {
            throw new UnsupportedOperationException("Only a single UUID is supported, you provided " + uuids.size());
        }
        String uploadId = getUploadId(uuids.get(0), job.getRestoreJobId().toString());
        ImportSSTableRequest.ImportOptions importOptions = new ImportSSTableRequest.ImportOptions();

        // Always verify SSTables on import
        importOptions.verifySSTables(true).extendedVerify(!job.skipExtendedVerify());

        try
        {
            QualifiedTableName qt = job.qualifiedTableName();
            SSTableImportResponse response =
            sidecarClient.importSSTableRequest(toSidecarInstance(instance),
                                               maybeQuotedIdentifier(bridge, qt.quoteIdentifiers(), qt.keyspace()),
                                               maybeQuotedIdentifier(bridge, qt.quoteIdentifiers(), qt.table()),
                                               uploadId,
                                               importOptions).get();
            if (response.success())
            {
                return new RemoteCommitResult(response.success(), Collections.emptyList(), Collections.singletonList(uploadId), null);
            }
            return new RemoteCommitResult(response.success(), Collections.singletonList(uploadId), Collections.emptyList(), null);
        }
        catch (ExecutionException | InterruptedException exception)
        {
            throw new SidecarApiCallException("Failed to commit sorted string tables", exception);
        }
    }

    @Override
    public void cleanUploadSession(CassandraInstance instance, String sessionID, String jobID) throws SidecarApiCallException
    {
        String uploadId = getUploadId(sessionID, jobID);
        try
        {
            sidecarClient.cleanUploadSession(toSidecarInstance(instance), uploadId).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.warn("Failed to clean upload uploadId={}, instance={}", uploadId, instance);
            throw new SidecarApiCallException("Failed to clean the upload session with ID " + uploadId, exception);
        }
    }

    protected String updateComponentName(Path componentFile, int ssTableIdx)
    {
        String[] ssTableNameParts = componentFile.getFileName().toString().split(SSTABLE_NAME_SEPARATOR);
        if (ssTableNameParts.length < SSTABLE_GENERATION_REVERSE_OFFSET)
        {
            throw new IllegalArgumentException("Invalid component file name: " + componentFile.getFileName());
        }
        ssTableNameParts[ssTableNameParts.length - SSTABLE_GENERATION_REVERSE_OFFSET] = Integer.toString(ssTableIdx);
        return String.join(SSTABLE_NAME_SEPARATOR, ssTableNameParts);
    }

    protected String getUploadId(String sessionID, String jobId)
    {
        return sessionID + "-" + jobId;
    }

    protected SidecarInstanceImpl toSidecarInstance(CassandraInstance instance)
    {
        return new SidecarInstanceImpl(instance.nodeName(), sidecarPort);
    }
}
