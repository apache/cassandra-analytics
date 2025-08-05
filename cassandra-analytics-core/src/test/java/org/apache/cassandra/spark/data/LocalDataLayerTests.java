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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaTests;
import org.apache.cassandra.spark.utils.ByteBufferUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalDataLayerTests extends VersionRunner
{

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testLocalDataLayer(CassandraBridge bridge) throws IOException
    {
        CassandraVersion version = bridge.getVersion();
        Path directory1 = Files.createTempDirectory("d1");
        Path directory2 = Files.createTempDirectory("d2");
        Path directory3 = Files.createTempDirectory("d3");
        Path directory4 = Files.createTempDirectory("d4");
        LocalDataLayer dataLayer = new LocalDataLayer(version, "backup_test", SchemaTests.SCHEMA,
                Stream.of(directory1, directory2, directory3, directory4)
                      .map(directory -> directory.toAbsolutePath().toString())
                      .toArray(String[]::new));
        assertThat(dataLayer.version()).isEqualTo(version);
        assertThat(dataLayer.partitionCount()).isEqualTo(1);
        assertThat(dataLayer.cqlTable()).isNotNull();
        assertThat(dataLayer.isInPartition(0, BigInteger.ZERO, ByteBuffer.wrap(ByteBufferUtils.EMPTY))).isTrue();
        assertThat(dataLayer.partitioner()).isEqualTo(Partitioner.Murmur3Partitioner);
        SSTablesSupplier ssTables = dataLayer.sstables(0, null, Collections.emptyList());
        assertThat(ssTables).isNotNull();
        assertThat(ssTables.openAll((ssTable, isRepairPrimary) -> null).isEmpty()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEquality(CassandraBridge bridge)
    {
        CassandraVersion version = bridge.getVersion();
        LocalDataLayer dataLayer1 = new LocalDataLayer(version, "backup_test", SchemaTests.SCHEMA,
                "/var/lib/cassandra/data1/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data2/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data3/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data4/data/backup_test/sbr_test/snapshot/snapshotName/");
        LocalDataLayer dataLayer2 = new LocalDataLayer(version, "backup_test", SchemaTests.SCHEMA,
                "/var/lib/cassandra/data1/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data2/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data3/data/backup_test/sbr_test/snapshot/snapshotName/",
                "/var/lib/cassandra/data4/data/backup_test/sbr_test/snapshot/snapshotName/");
        assertThat(dataLayer2).isNotSameAs(dataLayer1);
        assertThat(dataLayer1).isEqualTo(dataLayer1);
        assertThat(dataLayer2).isEqualTo(dataLayer2);
        assertThat(dataLayer2).isNotEqualTo(null);
        assertThat(dataLayer1).isNotEqualTo(new ArrayList<>());
        assertThat(dataLayer2).isEqualTo(dataLayer1);
        assertThat(dataLayer2.hashCode()).isEqualTo(dataLayer1.hashCode());
    }
}
