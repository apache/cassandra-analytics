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

import java.io.Serializable;
import java.util.UUID;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface JobInfo extends Serializable
{
    // ******************
    // Job Information API - should this really just move back to Config? Here to try to reduce the violations of the Law of Demeter more than anything else
    ConsistencyLevel getConsistencyLevel();

    @Nullable
    String getLocalDC();

    /**
     * @return the max sstable data file size in mebibytes
     */
    int sstableDataSizeInMiB();

    int getCommitBatchSize();

    int getCommitThreadsPerInstance();

    /**
     * return the identifier of the restore job created on Cassandra Sidecar
     * @return time-based uuid
     */
    UUID getRestoreJobId();

    /**
     * An optional unique identified supplied in spark configuration
     * @return a id string or null
     */
    String getConfiguredJobId();

    // Convenient method to decide a unique identified used for the job.
    // It prefers the configuredJobId if present; otherwise, fallback to the restoreJobId
    default String getId()
    {
        String configuredJobId = getConfiguredJobId();
        return configuredJobId == null ? getRestoreJobId().toString() : configuredJobId;
    }

    TokenPartitioner getTokenPartitioner();

    boolean skipExtendedVerify();

    boolean getSkipClean();

    /**
     * @return the digest type provider for the bulk job, and used to calculate digests for SSTable components
     */
    @NotNull
    DigestAlgorithmSupplier digestAlgorithmSupplier();

    QualifiedTableName qualifiedTableName();

    DataTransportInfo transportInfo();

    /**
     * @return job keep alive time in minutes
     */
    int jobKeepAliveMinutes();

    /**
     * @return job timeout in seconds; see {@link WriterOptions#JOB_TIMEOUT_SECONDS}
     */
    long jobTimeoutSeconds();

    /**
     * @return sidecar service port
     */
    int effectiveSidecarPort();

    /**
     * @return multiplier to calculate the final timeout for import coordinator
     */
    double importCoordinatorTimeoutMultiplier();

    /**
     * @return CoordinatedWriteConf if configured, null otherwise
     */
    @Nullable
    CoordinatedWriteConf coordinatedWriteConf();

    /**
     * @return true if coordinated write is enabled, i.e. coordinatedWriteConf() returns non-null value; false, otherwise
     */
    default boolean isCoordinatedWriteEnabled()
    {
        return coordinatedWriteConf() != null;
    }
}
