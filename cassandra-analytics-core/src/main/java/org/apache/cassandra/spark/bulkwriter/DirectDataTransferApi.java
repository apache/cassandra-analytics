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
import java.util.List;

import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.jetbrains.annotations.Nullable;

public interface DirectDataTransferApi
{
    RemoteCommitResult commitSSTables(CassandraInstance instance,
                                      String migrationId,
                                      List<String> uuids) throws SidecarApiCallException;

    void cleanUploadSession(CassandraInstance instance,
                            String sessionID,
                            String jobID) throws SidecarApiCallException;

    void uploadSSTableComponent(Path componentFile,
                                int ssTableIdx,
                                CassandraInstance instance,
                                String sessionID,
                                Digest digest) throws SidecarApiCallException;

    class RemoteCommitResult
    {
        public final boolean isSuccess;
        public final List<String> failedUuids;
        public final List<String> successfulUuids;
        public final String stdErr;

        public RemoteCommitResult(boolean isSuccess,
                                  @Nullable List<String> failedUuids,
                                  List<String> successfulUuids,
                                  @Nullable String stdErr)
        {
            this.isSuccess = isSuccess;
            this.failedUuids = failedUuids;
            this.successfulUuids = successfulUuids;
            this.stdErr = stdErr;
        }
    }
}
