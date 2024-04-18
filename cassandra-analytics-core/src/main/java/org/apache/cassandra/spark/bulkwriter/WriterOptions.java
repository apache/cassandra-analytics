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
    SIDECAR_INSTANCES,
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
    TRUSTSTORE_PASSWORD,
    TRUSTSTORE_TYPE,
    TRUSTSTORE_PATH,
    TRUSTSTORE_BASE64_ENCODED,
    SIDECAR_PORT,
    @Deprecated // the size unit `MB` is incorrect, use `SSTABLE_DATA_SIZE_IN_MIB` instead
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
     * Option to specify a comma separated list of Cassandra instances that are blocked from receiving any
     * writes from the analytics library. Note that this contributes to the number of unavailable instances for
     * the write consistency level validations.
     */
    BLOCKED_CASSANDRA_INSTANCES,
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
}
