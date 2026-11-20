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

import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;

/**
 * Spark options to configure bulk writer
 */
public enum WriterOptions implements WriterOption
{
    @Deprecated // Prefer the equivalent, SIDECAR_CONTACT_POINTS
    SIDECAR_INSTANCES,
    // The option specifies the initial contact points of sidecar servers to discover the cluster topology
    // Note that the addresses can include port; when port is present, it takes precedence over SIDECAR_PORT
    SIDECAR_CONTACT_POINTS,
    /**
     * The option specifies the configuration (in JSON) for coordinated write.
     * See org.apache.cassandra.spark.bulkwriter.coordinatedwrite.CoordinatedWriteConf.
     * When the option is present, SIDECAR_CONTACT_POINTS, SIDECAR_INSTANCES and LOCAL_DC are ignored if they are present.
     */
    COORDINATED_WRITE_CONFIG,
    KEYSPACE,
    TABLE,
    BULK_WRITER_CL,
    LOCAL_DC,
    NUMBER_SPLITS,
    COMMIT_THREADS_PER_INSTANCE,
    COMMIT_BATCH_SIZE,
    SKIP_EXTENDED_VERIFY,
    WRITE_MODE,
    KEYSTORE_PASSWORD,
    KEYSTORE_PATH,
    KEYSTORE_BASE64_ENCODED,
    KEYSTORE_TYPE,
    /**
     * Option that specifies cassandra role used for role based access control in Sidecar.
     * <p>
     * Role passed is verified on server side if specified role is present among the roles assigned to user in Cassandra database. Once verified,
     * server uses only the permissions associated with this intended role for authorization checks. If role is not set,
     * sidecar uses all assigned roles for user in database for permission evaluation.
     */
    CASSANDRA_ROLE,
    TRUSTSTORE_PASSWORD,
    TRUSTSTORE_TYPE,
    TRUSTSTORE_PATH,
    TRUSTSTORE_BASE64_ENCODED,
    SIDECAR_PORT,
    @Deprecated // Prefer the equivalent, `SSTABLE_DATA_SIZE_IN_MIB`. The size unit `MB` is incorrect and internally treated as MiB.
    SSTABLE_DATA_SIZE_IN_MB,
    SSTABLE_DATA_SIZE_IN_MIB,
    TTL,
    TIMESTAMP,
    /**
     * Option that specifies whether the identifiers (i.e. keyspace, table name, column names) should be quoted to
     * support mixed case and reserved keyword names for these fields.
     */
    QUOTE_IDENTIFIERS,
    /**
     * Option that specifies the type of digest to compute when uploading SSTables for checksum validation.
     * If unspecified, it defaults to {@code XXHash32} digests. The legacy {@code MD5} digest is also supported.
     */
    DIGEST,
    /**
     * Option to specify the data transport mode. It accepts either {@link DataTransport#DIRECT} or {@link DataTransport#S3_COMPAT}
     * Note that if S3_COMPAT is configured, {@link DATA_TRANSPORT_EXTENSION_CLASS} must be configured too.
     */
    DATA_TRANSPORT,
    /**
     * Option to specify the FQCN of class that implements {@link StorageTransportExtension}
     */
    DATA_TRANSPORT_EXTENSION_CLASS,
    /**
     * Option to tune the concurrency of S3 client's worker thread pool
     */
    STORAGE_CLIENT_CONCURRENCY,
    /**
     * Option to tune the thread keep alive seconds for the thread pool used in s3 client
     */
    STORAGE_CLIENT_THREAD_KEEP_ALIVE_SECONDS,
    /**
     * Option to specify the max chunk size for the multipart upload to S3
     */
    STORAGE_CLIENT_MAX_CHUNK_SIZE_IN_BYTES,
    /**
     * Option to specify the https proxy for s3 client
     */
    STORAGE_CLIENT_HTTPS_PROXY,
    /**
     * Option to specify the s3 server endpoint override; it is mostly used for testing
     */
    STORAGE_CLIENT_ENDPOINT_OVERRIDE,
    /**
     * Option to specify the maximum size of bundle (s3 object) to upload to s3
     */
    MAX_SIZE_PER_SSTABLE_BUNDLE_IN_BYTES_S3_TRANSPORT,
    /**
     * Option to specify the keep alive time in minutes for Sidecar to consider a job has lost/failed
     * after not receiving its heartbeat
     */
    JOB_KEEP_ALIVE_MINUTES,
    JOB_ID,
    /**
     * Option to tune the connection acquisition timeout for the nio http client employed in s3 client
     */
    STORAGE_CLIENT_NIO_HTTP_CLIENT_CONNECTION_ACQUISITION_TIMEOUT_SECONDS,
    /**
     * Option to tune the concurrency of the nio http client employed in s3 client
     */
    STORAGE_CLIENT_NIO_HTTP_CLIENT_MAX_CONCURRENCY,
    /**
     * Option to specify the timeout in seconds for bulk write jobs. By default, it is disabled.
     * When JOB_TIMEOUT_SECONDS is specified, a job exceeding the timeout is:
     * - successful when the desired consistency level is met
     * - a failure otherwise
     */
    JOB_TIMEOUT_SECONDS,
    /**
     * Option to bypass the secondary index validation check during bulk write job setup.
     * By default, bulk writes to tables with secondary indexes are rejected.
     * Setting this option to {@code true} allows bulk writes to proceed on tables that have secondary indexes,
     * with the understanding that the secondary indexes will NOT be updated by the bulk write and must be
     * rebuilt separately after the job completes.
     */
    SKIP_SECONDARY_INDEX_CHECK,
    /**
     * Option to specify the credential mechanism the sidecar should use to authenticate with S3.
     * Accepted values (case-insensitive): {@code "STATIC"}, {@code "IAM"}.
     * <p>
     * When absent the sidecar defaults to static credentials (backward-compatible behavior).
     * <ul>
     *   <li>{@code STATIC}: send full credentials; the sidecar uses {@code StaticCredentialsProvider}.</li>
     *   <li>{@code IAM}: send region only; the sidecar uses {@code DefaultCredentialsProvider}
     *       (instance profile / IRSA / ECS task role). The {@link StorageTransportExtension} should
     *       return a {@code StorageCredentialPair} created with
     *       {@code StorageCredentialPair.iamPair(readRegion, writeRegion)}.</li>
     * </ul>
     */
    STORAGE_CREDENTIAL_TYPE,
    /**
     * Option to enable Sidecar fast forward for coordinated write, i.e. for all clusters in
     * {@link #COORDINATED_WRITE_CONFIG}. It is only valid when {@link #COORDINATED_WRITE_CONFIG} is present, and it
     * defaults to {@code false}.
     * <p>
     * When enabled, Sidecar stages each slice once its object is available in the cluster's read bucket, instead of
     * waiting for the {@link StorageTransportExtension} to signal stage-ready; if the object has not been replicated
     * there yet, Sidecar retries until it is. It imports as soon as the job reaches {@code STAGED}, instead of waiting
     * for {@code IMPORT_READY}. The driver sets {@code STAGED} only after all clusters have staged and satisfied the
     * consistency level.
     * <p>
     * Only enable it when it is acceptable that the imported data becomes visible immediately and that importing
     * cannot be rolled back. With fast forward, {@code STAGED} is the point of no return, instead of
     * {@code IMPORT_READY}. The driver sends {@code STAGED} to the clusters one at a time, and each cluster starts
     * importing when it receives it. If the job fails, is cancelled or times out after that, the job is reported as
     * failed, but the data already imported to any cluster stays. The import-ready decision of the
     * {@link StorageTransportExtension} cannot stop the import either, although the driver still waits for it before
     * reporting the job as successful.
     * <p>
     * It requires the Sidecars of all clusters to support fast forward (CASSSIDECAR-463). Sidecar versions without the
     * support do not accept unknown fields in the create restore job request, so the job creation fails.
     */
    COORDINATED_WRITE_FAST_FORWARD_ENABLED,
}
