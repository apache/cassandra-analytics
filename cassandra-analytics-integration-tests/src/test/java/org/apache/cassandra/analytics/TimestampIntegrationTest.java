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

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.TimestampOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the Cassandra timestamps
 */
@ExtendWith(VertxExtension.class)
public class TimestampIntegrationTest extends SparkIntegrationTestBase
{
    public static final String CREATE_TABLE_SCHEMA = "CREATE TABLE IF NOT EXISTS %s " +
                                                     "(id BIGINT PRIMARY KEY, course TEXT, marks BIGINT);";
    public static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

    /**
     * Reads from source table with timestamps, and then persist the read data to the target
     * table using the timestamp as input
     */
    @CassandraIntegrationTest(nodesPerDc = 2)
    void testReadingAndWritingTimestamp()
    {
        QualifiedName sourceTableName = uniqueTestTableFullName(TEST_KEYSPACE, "source_tbl");
        QualifiedName targetTableName = uniqueTestTableFullName(TEST_KEYSPACE, "target_tbl");

        createTestKeyspace(sourceTableName.maybeQuotedKeyspace(), ImmutableMap.of("datacenter1", 1));
        createTestTable(String.format(CREATE_TABLE_SCHEMA, sourceTableName));
        createTestTable(String.format(CREATE_TABLE_SCHEMA, targetTableName));
        populateTable(sourceTableName, DATASET);
        waitUntilSidecarPicksUpSchemaChange(sourceTableName.maybeQuotedKeyspace());
        waitUntilSidecarPicksUpSchemaChange(targetTableName.maybeQuotedKeyspace());

        Dataset<Row> data = bulkReaderDataFrame(sourceTableName).option("lastModifiedColumnName", "lm")
                                                                .load();
        assertThat(data.count()).isEqualTo(DATASET.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getLong(0)))
                                .collect(Collectors.toList());

        bulkWriterDataFrameWriter(data, targetTableName).option(WriterOptions.TIMESTAMP.name(), TimestampOption.perRow("lm"))
                                                        .save();
        validateWrites(targetTableName, rowList);
    }

    void validateWrites(QualifiedName tableName, List<Row> sourceData)
    {
        // build a set of entries read from Cassandra into a set
        Set<String> actualEntries = Arrays.stream(sidecarTestContext.cassandraTestContext()
                                                                    .cluster()
                                                                    .coordinator(1)
                                                                    .execute(String.format("SELECT id, course, marks FROM %s;", tableName), ConsistencyLevel.LOCAL_QUORUM))
                                          .map((Object[] columns) -> String.format("%s:%s:%s:1432815430948567",
                                                                                   columns[0],
                                                                                   columns[1],
                                                                                   columns[2]))
                                          .collect(Collectors.toSet());

        // Number of entries in Cassandra must match the original datasource
        assertThat(actualEntries.size()).isEqualTo(sourceData.size());

        // remove from actual entries to make sure that the data read is the same as the data written
        sourceData.forEach(row -> {
            Instant instant = row.getTimestamp(3).toInstant();
            long timeInMicros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
            String key = String.format("%d:%s:%d:%s",
                                       row.getLong(0),
                                       row.getString(1),
                                       row.getLong(2),
                                       timeInMicros);
            assertThat(actualEntries.remove(key)).as(key + " is expected to exist in the actual entries")
                                                 .isTrue();
        });

        // If this fails, it means there was more data in the database than we expected
        assertThat(actualEntries).as("All entries are expected to be read from database")
                                 .isEmpty();
    }

    void populateTable(QualifiedName tableName, List<String> values)
    {
        ICoordinator coordinator = sidecarTestContext.cassandraTestContext()
                                                     .cluster()
                                                     .getFirstRunningInstance()
                                                     .coordinator();
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            String query = String.format("INSERT INTO %s (id, course, marks) VALUES (%d,'%s',%d) USING TIMESTAMP 1432815430948567",
                                         tableName, i, "course_" + value, 80 + i);
            coordinator.execute(query, ConsistencyLevel.ALL);
        }
    }
}
