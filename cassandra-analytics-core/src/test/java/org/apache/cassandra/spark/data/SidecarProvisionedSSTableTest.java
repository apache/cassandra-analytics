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

package org.apache.cassandra.spark.data;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import o.a.c.sidecar.client.shaded.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.stats.Stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link SidecarProvisionedSSTable}
 */
class SidecarProvisionedSSTableTest
{
    SidecarClient mockSidecarClient;
    private Sidecar.ClientConfig sidecarClientConfig;

    @BeforeEach
    void setup()
    {
        mockSidecarClient = mock(SidecarClient.class);
        sidecarClientConfig = Sidecar.ClientConfig.create();
    }

    // SidecarProvisionedSSTable are cached in SSTableCache, so we need to correctly implement
    // equality and hash code
    @Test
    void testEqualityAndHashCode()
    {
        SSTable ssTable10 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable20 = prepareTable("localhost1", 9043, "keyspace2", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable30 = prepareTable("localhost1", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable40 = prepareTable("localhost2", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable50 = prepareTable("localhost1", 9044, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable60 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-2-big-Data.db");

        // These are the same as the previous SSTables
        SSTable ssTable11 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable21 = prepareTable("localhost1", 9043, "keyspace2", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable31 = prepareTable("localhost1", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable41 = prepareTable("localhost2", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable51 = prepareTable("localhost1", 9044, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable61 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-2-big-Data.db");

        assertThat(ssTable10).isNotEqualTo(ssTable20)
                             .isNotEqualTo(ssTable30)
                             .isNotEqualTo(ssTable40)
                             .isNotEqualTo(ssTable50)
                             .isNotEqualTo(ssTable60);

        assertThat(ssTable20).isNotEqualTo(ssTable30)
                             .isNotEqualTo(ssTable40)
                             .isNotEqualTo(ssTable50)
                             .isNotEqualTo(ssTable60);

        assertThat(ssTable30).isNotEqualTo(ssTable40)
                             .isNotEqualTo(ssTable50)
                             .isNotEqualTo(ssTable60);

        assertThat(ssTable40).isNotEqualTo(ssTable50)
                             .isNotEqualTo(ssTable60);

        assertThat(ssTable50).isNotEqualTo(ssTable60);

        assertThat(ssTable10).isEqualTo(ssTable11);
        assertThat(ssTable20).isEqualTo(ssTable21);
        assertThat(ssTable30).isEqualTo(ssTable31);
        assertThat(ssTable40).isEqualTo(ssTable41);
        assertThat(ssTable50).isEqualTo(ssTable51);
        assertThat(ssTable60).isEqualTo(ssTable61);
    }

    @Test
    void testToString()
    {
        SSTable ssTable10 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable20 = prepareTable("localhost1", 9043, "keyspace2", "table1", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable30 = prepareTable("localhost1", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable40 = prepareTable("localhost2", 9043, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable50 = prepareTable("localhost1", 9044, "keyspace1", "table2", "snapshot1", "na-1-big-Data.db");
        SSTable ssTable60 = prepareTable("localhost1", 9043, "keyspace1", "table1", "snapshot1", "na-2-big-Data.db");

        assertThat(ssTable10.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost1', port=9043, " +
                                                   "keyspace='keyspace1', table='table1', snapshotName='snapshot1', " +
                                                   "dataFileName='na-1-big-Data.db', partitionId=1}");

        assertThat(ssTable20.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost1', port=9043, " +
                                                   "keyspace='keyspace2', table='table1', snapshotName='snapshot1', " +
                                                   "dataFileName='na-1-big-Data.db', partitionId=1}");

        assertThat(ssTable30.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost1', port=9043, " +
                                                   "keyspace='keyspace1', table='table2', snapshotName='snapshot1', " +
                                                   "dataFileName='na-1-big-Data.db', partitionId=1}");

        assertThat(ssTable40.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost2', port=9043, " +
                                                   "keyspace='keyspace1', table='table2', snapshotName='snapshot1', " +
                                                   "dataFileName='na-1-big-Data.db', partitionId=1}");

        assertThat(ssTable50.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost1', port=9044, " +
                                                   "keyspace='keyspace1', table='table2', snapshotName='snapshot1', " +
                                                   "dataFileName='na-1-big-Data.db', partitionId=1}");

        assertThat(ssTable60.toString()).isEqualTo("SidecarProvisionedSSTable{hostname='localhost1', port=9043, " +
                                                   "keyspace='keyspace1', table='table1', snapshotName='snapshot1', " +
                                                   "dataFileName='na-2-big-Data.db', partitionId=1}");
    }

    @ParameterizedTest
    @ValueSource(strings = { "bad1bigData.db", "na-1-big.db" })
    void failsOnBadDataFileName(String dataFileName)
    {
        assertThatExceptionOfType(ArrayIndexOutOfBoundsException.class)
        .isThrownBy(() -> prepareTable("localhost", 9043, "ks", "tbl", "snap", dataFileName));
    }

    SSTable prepareTable(String sidecarHostName,
                         int sidecarPort,
                         String keyspace,
                         String table,
                         String snapshot,
                         String dataFileName)
    {
        ListSnapshotFilesResponse.FileInfo fileInfo = new ListSnapshotFilesResponse.FileInfo(5,
                                                                                             sidecarHostName,
                                                                                             sidecarPort,
                                                                                             1,
                                                                                             snapshot,
                                                                                             keyspace,
                                                                                             table + "-abc1234",
                                                                                             dataFileName);
        return new SidecarProvisionedSSTable(mockSidecarClient,
                                             sidecarClientConfig,
                                             new SidecarInstanceImpl(sidecarHostName, sidecarPort),
                                             keyspace,
                                             table,
                                             snapshot,
                                             Collections.singletonMap(FileType.DATA, fileInfo),
                                             1,
                                             Stats.DoNothingStats.INSTANCE);
    }
}
