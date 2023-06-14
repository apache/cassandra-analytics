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

package org.apache.cassandra.analytics;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

public class SparkBulkWriterSimpleTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 3)
    public void runSampleJob() throws InterruptedException
    {
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        String keyspace = "spark_test";
        String table = "test";
        cluster.schemaChange(
        "  CREATE KEYSPACE " + keyspace + " WITH replication = "
        + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
        + "      AND durable_writes = true;");
        cluster.schemaChange("CREATE TABLE " + keyspace + "." + table + " (\n"
                             + "          id BIGINT PRIMARY KEY,\n"
                             + "          course BLOB,\n"
                             + "          marks BIGINT\n"
                             + "     );");
        waitForKeyspaceAndTable(keyspace, table);
        IntegrationTestJob.main(new String[]{String.valueOf(server.actualPort())});
    }
}
