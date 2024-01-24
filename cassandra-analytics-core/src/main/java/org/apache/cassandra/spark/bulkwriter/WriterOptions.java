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
}
