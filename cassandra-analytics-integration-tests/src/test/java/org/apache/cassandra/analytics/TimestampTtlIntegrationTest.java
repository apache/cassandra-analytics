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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.TimestampOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the Cassandra timestamps and TTLs
 */
class TimestampTtlIntegrationTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

    // table contains rows with custom TIMESTAMP and TTL
    static final QualifiedName SOURCE_TABLE = uniqueTestTableFullName(TEST_KEYSPACE, "source_tbl");

    // table contains rows with custom TIMESTAMP only
    static final QualifiedName NO_TTL_TABLE = uniqueTestTableFullName(TEST_KEYSPACE, "no_ttl_tbl");

    // table contains rows whose cells contain different TTL value
    static final QualifiedName VARIABLE_TTL_TABLE = uniqueTestTableFullName(TEST_KEYSPACE, "variable_ttl_tbl");

    static final QualifiedName TARGET_TABLE = uniqueTestTableFullName(TEST_KEYSPACE, "target_tbl");

    static final List<QualifiedName> TABLE_NAMES = Arrays.asList(SOURCE_TABLE,
                                                                 TARGET_TABLE,
                                                                 NO_TTL_TABLE,
                                                                 VARIABLE_TTL_TABLE);

    static final long desiredTimestamp = 1432815430948567L;
    static final int desiredTtl = 600;

    /**
     * Reads from source table with timestamps, and then persist the read data to the target
     * table using the timestamp as input
     */
    @Test
    void testReadingAndWritingTimestamp()
    {
        Dataset<Row> data = bulkReaderDataFrame(SOURCE_TABLE).option("lastModifiedColumnName", "lm")
                                                             .load();
        assertThat(data.count()).isEqualTo(DATASET.size());
        List<Row> rowList = data.collectAsList().stream()
                                .sorted(Comparator.comparing(row -> row.getInt(0)))
                                .collect(Collectors.toList());

        bulkWriterDataFrameWriter(data, TARGET_TABLE).option(WriterOptions.TIMESTAMP.name(), TimestampOption.perRow("lm"))
                                                     .save();
        validateWrites(TARGET_TABLE, rowList);
    }

    @Test
    void testReadingCellTimestampAndTtl() throws Exception
    {
        Thread.sleep(2000); // elapse two seconds so that TTL differs

        Dataset<Row> data = bulkReaderDataFrame(SOURCE_TABLE).option("lastModifiedTimestamp_course", "courseTimestamp")
                                                             .option("ttl_course", "courseTtl")
                                                             .option("lastModifiedTimestamp_marks", "marksTimestamp")
                                                             .option("ttl_marks", "marksTtl")
                                                             .load()
                                                             .select("id", "courseTimestamp", "courseTtl", "marksTimestamp", "marksTtl");

        List<Row> rows = data.collectAsList();

        assertThat(rows).hasSize(DATASET.size());

        rows.forEach(row -> {
            Instant timestamp = row.getTimestamp(1).toInstant();
            assertThat(timestamp.getEpochSecond()).isEqualTo(1432815430L);
            assertThat(timestamp.getNano()).isEqualTo(948567000L);

            int ttl = row.getInt(2);
            assertThat(ttl).isBetween(1, 599);

            assertThat(row.getTimestamp(3)).isNotNull();
            assertThat(row.getInt(4)).isNotNull();
        });
    }

    @Test
    void testReadingCellWithoutTtl() throws Exception
    {
        populateTable(NO_TTL_TABLE, DATASET, desiredTimestamp, CqlField.NO_TTL);
        Dataset<Row> data = bulkReaderDataFrame(NO_TTL_TABLE).option("ttl_course", "courseTtl")
                                                             .option("ttl_marks", "marksTtl")
                                                             .load()
                                                             .select("id", "courseTtl", "marksTtl");

        List<Row> rows = data.collectAsList();

        assertThat(rows).hasSize(DATASET.size());

        rows.forEach(row -> {
            assertThat(row.isNullAt(1)).isTrue();
            assertThat(row.isNullAt(2)).isTrue();
        });
    }

    @Test
    void testReadingRowWithVariableTtlAndTimestamp() throws Exception
    {
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        String query = String.format("INSERT INTO %s (id, course, marks) VALUES (%d,'%s',%d) USING TTL %d",
                                     VARIABLE_TTL_TABLE, 1, "course_a", 2, desiredTtl);
        coordinator.execute(query, ConsistencyLevel.ALL);

        // update TTL of "marks" column for TTL to differ
        query = String.format("UPDATE %s USING TTL %d SET marks = %d WHERE id = %d",
                              VARIABLE_TTL_TABLE, desiredTtl / 2, 3, 1);
        Thread.sleep(2000);
        coordinator.execute(query, ConsistencyLevel.ALL);

        Dataset<Row> data = bulkReaderDataFrame(VARIABLE_TTL_TABLE).option("lastModifiedTimestamp_course", "courseTimestamp")
                                                                   .option("ttl_course", "courseTtl")
                                                                   .option("lastModifiedTimestamp_marks", "marksTimestamp")
                                                                   .option("ttl_marks", "marksTtl")
                                                                   .load()
                                                                   .select("id", "courseTimestamp", "courseTtl", "marksTimestamp", "marksTtl");

        List<Row> rows = data.collectAsList();

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        // write timestamp of "course" should be earlier than "marks"
        assertThat(row.getTimestamp(1).toInstant()).isBefore(row.getTimestamp(3).toInstant());
        // TTL of "marks" column has been decreased with UPDATE statement
        assertThat(row.getInt(2)).isGreaterThan(row.getInt(4));
    }

    @Override
    protected void initializeSchemaForTest()
    {
        TABLE_NAMES.forEach(name -> {
            createTestKeyspace(name, DC1_RF1);
            createTestTable(name, CREATE_TEST_TABLE_STATEMENT);
        });
        populateTable(SOURCE_TABLE, DATASET, desiredTimestamp, desiredTtl);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    void validateWrites(QualifiedName tableName, List<Row> sourceData)
    {
        // build a set of entries read from Cassandra into a set
        // the writetime function must read the timestamp specified for the test
        // to ensure that the persisted timestamp is correct
        String query = String.format("SELECT id, course, marks, WRITETIME(course) FROM %s;", tableName);
        Set<String> actualEntries = Arrays.stream(cluster.coordinator(1)
                                                         .execute(String.format(query, tableName), ConsistencyLevel.ALL))
                                          .map((Object[] columns) -> String.format("%s:%s:%s:%s",
                                                                                   columns[0],
                                                                                   columns[1],
                                                                                   columns[2],
                                                                                   columns[3]))
                                          .collect(Collectors.toSet());

        // Number of entries in Cassandra must match the original datasource
        assertThat(actualEntries.size()).isEqualTo(sourceData.size());

        // remove from actual entries to make sure that the data read is the same as the data written
        sourceData.forEach(row -> {
            Instant instant = row.getTimestamp(3).toInstant();
            long timeInMicros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
            String key = String.format("%d:%s:%d:%s",
                                       row.getInt(0),
                                       row.getString(1),
                                       row.getInt(2),
                                       timeInMicros);
            assertThat(actualEntries.remove(key)).as(key + " is expected to exist in the actual entries")
                                                 .isTrue();
        });

        // If this fails, it means there was more data in the database than we expected
        assertThat(actualEntries).as("All entries are expected to be read from database")
                                 .isEmpty();
    }

    void populateTable(QualifiedName tableName, List<String> values, long desiredTimestamp, int desiredTtl)
    {
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        for (int i = 0; i < values.size(); i++)
        {
            String value = values.get(i);
            String query = "INSERT INTO %s (id, course, marks) VALUES (%d,'%s',%d) USING TIMESTAMP %d";
            List<Object> variables = new ArrayList<>(Arrays.asList(tableName, i, "course_" + value, 80 + i, desiredTimestamp));
            if (desiredTtl != CqlField.NO_TTL)
            {
                query += " AND TTL %d";
                variables.add(desiredTtl);
            }
            query = String.format(query, variables.toArray());
            coordinator.execute(query, ConsistencyLevel.ALL);
        }
    }
}
