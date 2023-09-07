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

import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.request.ImportSSTableRequest;
import org.apache.cassandra.sidecar.common.data.SSTableImportResponse;
import org.apache.cassandra.spark.common.MD5Hash;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.validation.CassandraValidation;
import org.apache.cassandra.spark.validation.SidecarValidation;
import org.apache.cassandra.spark.validation.StartupValidator;

/**
 * A {@link DataTransferApi} implementation that interacts with Cassandra Sidecar
 */
public class SidecarDataTransferApi implements DataTransferApi
{
    private static final long serialVersionUID = 2563347232666882754L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarDataTransferApi.class);
    private static final String SSTABLE_NAME_SEPARATOR = "-";
    private static final int SSTABLE_GENERATION_REVERSE_OFFSET = 3;

    private final transient SidecarClient sidecarClient;
    private final JobInfo job;
    private final BulkSparkConf conf;

    public SidecarDataTransferApi(SidecarClient sidecarClient, JobInfo job, BulkSparkConf conf)
    {
        this.sidecarClient = sidecarClient;
        this.job = job;
        this.conf = conf;
    }

    @Override
    public void uploadSSTableComponent(Path componentFile,
                                       int ssTableIdx,
                                       CassandraInstance instance,
                                       String sessionID,
                                       MD5Hash fileHash) throws ClientException
    {
        String componentName = updateComponentName(componentFile, ssTableIdx);
        String uploadId = getUploadId(sessionID, job.getId().toString());
        try
        {
            sidecarClient.uploadSSTableRequest(toSidecarInstance(instance), conf.keyspace, conf.table, uploadId,
                                               componentName, fileHash.toString(),
                                               componentFile.toAbsolutePath().toString()).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.warn("Failed to upload file={}, keyspace={}, table={}, uploadId={}, componentName={}, instance={}",
                        componentFile, conf.keyspace, conf.table, uploadId, componentName, instance);
            throw new ClientException(
                    String.format("Failed to upload file=%s into keyspace=%s, table=%s, componentName=%s with uploadId=%s to instance=%s",
                                  componentFile, conf.keyspace, conf.table, componentName, uploadId, instance), exception);
        }
    }

    @Override
    public RemoteCommitResult commitSSTables(CassandraInstance instance,
                                             String migrationId,
                                             List<String> uuids) throws ClientException
    {
        if (uuids.size() != 1)
        {
            throw new UnsupportedOperationException("Only a single UUID is supported, you provided " + uuids.size());
        }
        String uploadId = getUploadId(uuids.get(0), job.getId().toString());
        ImportSSTableRequest.ImportOptions importOptions = new ImportSSTableRequest.ImportOptions();

        if (job.validateSSTables())
        {
            importOptions.verifySSTables(true)
                         .extendedVerify(!job.skipExtendedVerify());
        }

        try
        {
            SSTableImportResponse response =
            sidecarClient.importSSTableRequest(toSidecarInstance(instance), conf.keyspace, conf.table, uploadId, importOptions).get();
            if (response.success())
            {
                return new RemoteCommitResult(response.success(), Collections.emptyList(), Collections.singletonList(uploadId), null);
            }
            return new RemoteCommitResult(response.success(), Collections.singletonList(uploadId), Collections.emptyList(), null);
        }
        catch (ExecutionException | InterruptedException exception)
        {
            throw new ClientException("Failed to commit sorted string tables", exception);
        }
    }

    @Override
    public void cleanUploadSession(CassandraInstance instance, String sessionID, String jobID) throws ClientException
    {
        String uploadId = getUploadId(sessionID, jobID);
        try
        {
            sidecarClient.cleanUploadSession(toSidecarInstance(instance), uploadId).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.warn("Failed to clean upload uploadId={}, instance={}", uploadId, instance);
            throw new ClientException("Failed to clean the upload session with ID " + uploadId, exception);
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

    private SidecarInstanceImpl toSidecarInstance(CassandraInstance instance)
    {
        return new SidecarInstanceImpl(instance.getNodeName(), conf.getSidecarPort());
    }
}
