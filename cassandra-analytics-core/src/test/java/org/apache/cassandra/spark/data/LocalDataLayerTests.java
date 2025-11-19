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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaTests;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.utils.ByteBufferUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static com.google.common.collect.BoundType.CLOSED;

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

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testTimeRangeFilterFromOptions(CassandraBridge bridge)
    {
        String schemaWithTWCS = schemaWithTWCS();

        Map<String, String> options = new HashMap<>();
        options.put("version", bridge.getVersion().name());
        options.put("partitioner", Partitioner.Murmur3Partitioner.name());
        options.put("keyspace", "test_keyspace");
        options.put("createstmt", schemaWithTWCS);
        options.put("dirs", "/tmp/data1,/tmp/data2");
        options.put("sstable_start_timestamp_micros", "1000");
        options.put("sstable_end_timestamp_micros", "2000");

        LocalDataLayer dataLayer = LocalDataLayer.from(options);

        SSTableTimeRangeFilter filter = dataLayer.sstableTimeRangeFilter();
        assertThat(filter.range().lowerEndpoint()).isEqualTo(1000L);
        assertThat(filter.range().upperEndpoint()).isEqualTo(2000L);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testTimeRangeFilterNotSupportedWithLCS(CassandraBridge bridge)
    {
        String schemaWithLeveledCompaction = "CREATE TABLE test_keyspace.test_table (\n"
                                           + "    id uuid,\n"
                                           + "    value text,\n"
                                           + "    PRIMARY KEY(id)\n"
                                           + ") WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}";

        CassandraVersion version = bridge.getVersion();
        SSTableTimeRangeFilter filter = SSTableTimeRangeFilter.create(1000L, 2000L);

        assertThatThrownBy(() -> new LocalDataLayer(
            version,
            Partitioner.Murmur3Partitioner,
            "test_keyspace",
            schemaWithLeveledCompaction,
            Collections.emptySet(),
            Collections.emptyList(),
            false,
            null,
            filter,
            "/tmp/data1", "/tmp/data2"
        ))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("SSTableTimeRangeFilter is only supported with TimeWindowCompactionStrategy. " +
                              "Current compaction strategy is: org.apache.cassandra.db.compaction.LeveledCompactionStrategy");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSerializationWithTimeRangeFilter(CassandraBridge bridge) throws Exception
    {
        // Use TimeWindowCompactionStrategy since time range filters are only supported with TWCS
        String schemaWithTWCS = schemaWithTWCS();

        CassandraVersion version = bridge.getVersion();
        SSTableTimeRangeFilter filter = SSTableTimeRangeFilter.create(1000L, 2000L);
        LocalDataLayer dataLayer = new LocalDataLayer(
            version,
            Partitioner.Murmur3Partitioner,
            "test_keyspace",
            schemaWithTWCS,
            Collections.emptySet(),
            Collections.emptyList(),
            false,
            null,
            filter,
            "/tmp/data1", "/tmp/data2"
        );

        ByteArrayOutputStream baos = serialize(dataLayer);
        LocalDataLayer deserialized = deserialize(baos);

        SSTableTimeRangeFilter deserializedFilter = deserialized.sstableTimeRangeFilter();
        assertThat(deserializedFilter).isEqualTo(filter);
        assertThat(deserializedFilter.range().lowerEndpoint()).isEqualTo(1000L);
        assertThat(deserializedFilter.range().upperEndpoint()).isEqualTo(2000L);
        assertThat(deserializedFilter.range().lowerBoundType()).isEqualTo(CLOSED);
        assertThat(deserializedFilter.range().upperBoundType()).isEqualTo(CLOSED);
    }

    private ByteArrayOutputStream serialize(LocalDataLayer dataLayer) throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(dataLayer);
        oos.close();
        return baos;
    }

    private LocalDataLayer deserialize(ByteArrayOutputStream baos) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        LocalDataLayer deserialized = (LocalDataLayer) ois.readObject();
        ois.close();
        return deserialized;
    }

    private String schemaWithTWCS()
    {
        return "CREATE TABLE test_keyspace.test_table2 (\n"
               + "    id uuid,\n"
               + "    value text,\n"
               + "    PRIMARY KEY(id)\n"
               + ") WITH compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}";
    }
}
