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

package org.apache.cassandra.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.bridge.CassandraBridgeFactory.getSparkSql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public final class Tester
{
    static final int DEFAULT_NUM_ROWS = 200;

    @NotNull
    private final List<CassandraVersion> versions;
    @Nullable
    private final TestSchema.Builder schemaBuilder;
    @Nullable
    private final Function<String, TestSchema.Builder> schemaBuilderFunc;
    private final int numRandomRows;
    private final int expectedRowCount;
    @NotNull
    private final List<Consumer<TestSchema.TestRow>> writeListeners;
    @NotNull
    private final List<Consumer<TestSchema.TestRow>> readListeners;
    @NotNull
    private final List<Writer> writers;
    @NotNull
    private final List<Consumer<Dataset<Row>>> checks;
    @NotNull
    private final List<Integer> numSSTables;
    @Nullable
    private final Runnable reset;
    @Nullable
    private final String filterExpression;
    @Nullable
    private final String[] columns;
    @NotNull
    private final Set<String> sumFields;
    private final boolean shouldCheckNumSSTables;
    private final boolean addLastModifiedTimestamp;
    private final int delayBetweenSSTablesInSecs;
    private final String statsClass;
    private final boolean upsert;
    private final boolean nullifyValueColumn;

    private Tester(Builder builder)
    {
        this.versions = builder.versions;
        this.schemaBuilder = builder.schemaBuilder;
        this.schemaBuilderFunc = builder.schemaBuilderFunc;
        this.numSSTables = builder.numSSTables;
        this.writeListeners = builder.writeListeners;
        this.readListeners = builder.readListeners;
        this.writers = builder.writers;
        this.checks = builder.checks;
        this.sumFields = builder.sumFields;
        this.reset = builder.reset;
        this.filterExpression = builder.filterExpression;
        this.numRandomRows = builder.numRandomRows;
        this.expectedRowCount = builder.expectedRowCount;
        this.shouldCheckNumSSTables = builder.shouldCheckNumSSTables;
        this.columns = builder.columns;
        this.addLastModifiedTimestamp = builder.addLastModifiedTimestamp;
        this.delayBetweenSSTablesInSecs = builder.delayBetweenSSTablesInSecs;
        this.statsClass = builder.statsClass;
        this.upsert = builder.upsert;
        this.nullifyValueColumn = builder.nullRegularColumns;
    }

    public static Builder builder(@NotNull TestSchema.Builder schemaBuilder)
    {
        return new Builder(schemaBuilder);
    }

    public static Builder builder(@NotNull Function<String, TestSchema.Builder> schemaBuilderFunc)
    {
        return new Builder(schemaBuilderFunc);
    }

    static class Writer
    {
        final Consumer<CassandraBridge.Writer> consumer;
        final boolean isTombstoneWriter;

        Writer(Consumer<CassandraBridge.Writer> consumer)
        {
            this(consumer, false);
        }

        Writer(Consumer<CassandraBridge.Writer> consumer, boolean isTombstoneWriter)
        {
            this.consumer = consumer;
            this.isTombstoneWriter = isTombstoneWriter;
        }
    }

    public static final class Builder
    {
        // TODO: Make use of TestUtils.testableVersions() instead
        @NotNull
        private List<CassandraVersion> versions = ImmutableList.of(CassandraVersion.FOURZERO);
        @Nullable
        private TestSchema.Builder schemaBuilder;
        @Nullable
        private Function<String, TestSchema.Builder> schemaBuilderFunc;
        private int numRandomRows = DEFAULT_NUM_ROWS;
        private int expectedRowCount = -1;
        @NotNull
        private final List<Consumer<TestSchema.TestRow>> writeListeners = new ArrayList<>();
        @NotNull
        private final List<Consumer<TestSchema.TestRow>> readListeners = new ArrayList<>();
        @NotNull
        private final List<Writer> writers = new ArrayList<>();
        @NotNull
        private final List<Consumer<Dataset<Row>>> checks = new ArrayList<>();
        @Nullable
        private Runnable reset = null;
        @NotNull
        private List<Integer> numSSTables = ImmutableList.of(1, 2, 5);
        @NotNull
        private Set<String> sumFields = Collections.emptySet();
        @Nullable
        private String filterExpression;
        @Nullable
        private String[] columns = null;
        private boolean shouldCheckNumSSTables = true;
        private boolean addLastModifiedTimestamp = false;
        private int delayBetweenSSTablesInSecs = 0;
        private String statsClass = null;
        private boolean upsert = false;
        private boolean nullRegularColumns = false;

        private Builder(@NotNull TestSchema.Builder schemaBuilder)
        {
            this.schemaBuilder = schemaBuilder;
        }

        private Builder(@NotNull Function<String, TestSchema.Builder> schemaBuilderFunc)
        {
            this.schemaBuilderFunc = schemaBuilderFunc;
        }

        // Runs a test for every Cassandra version given
        Builder withVersions(@NotNull Collection<CassandraVersion> versions)
        {
            this.versions = ImmutableList.copyOf(versions);
            return this;
        }

        // Runs a test for every number of SSTables given
        Builder withNumRandomSSTables(Integer... numSSTables)
        {
            this.numSSTables = ImmutableList.copyOf(numSSTables);
            return this;
        }

        Builder withSumField(String... fields)
        {
            sumFields = ImmutableSet.copyOf(fields);
            return this;
        }

        Builder withNumRandomRows(int numRow)
        {
            numRandomRows = numRow;
            return this;
        }

        Builder dontWriteRandomData()
        {
            numSSTables = ImmutableList.of(0);
            numRandomRows = 0;
            return this;
        }

        Builder withWriteListener(@Nullable Consumer<TestSchema.TestRow> writeListener)
        {
            if (writeListener != null)
            {
                writeListeners.add(writeListener);
            }
            return this;
        }

        Builder withReadListener(@Nullable Consumer<TestSchema.TestRow> readListener)
        {
            if (readListener != null)
            {
                readListeners.add(readListener);
            }
            return this;
        }

        Builder withSSTableWriter(@Nullable Consumer<CassandraBridge.Writer> consumer)
        {
            if (consumer != null)
            {
                writers.add(new Writer(consumer));
            }
            return this;
        }

        Builder withTombstoneWriter(@Nullable Consumer<CassandraBridge.Writer> consumer)
        {
            if (consumer != null)
            {
                writers.add(new Writer(consumer, true));
            }
            return this;
        }

        Builder withCheck(@Nullable Consumer<Dataset<Row>> check)
        {
            if (check != null)
            {
                checks.add(check);
            }
            return this;
        }

        Builder withExpectedRowCountPerSSTable(int expectedRowCount)
        {
            this.expectedRowCount = expectedRowCount;
            return this;
        }

        Builder withReset(Runnable reset)
        {
            this.reset = reset;
            return this;
        }

        Builder withFilter(@NotNull String filterExpression)
        {
            this.filterExpression = filterExpression;
            return this;
        }

        Builder withColumns(@NotNull String... columns)
        {
            this.columns = columns;
            return this;
        }

        Builder dontCheckNumSSTables()
        {
            shouldCheckNumSSTables = false;
            return this;
        }

        Builder withLastModifiedTimestampColumn()
        {
            addLastModifiedTimestamp = true;
            return this;
        }

        Builder withDelayBetweenSSTablesInSecs(int delay)
        {
            delayBetweenSSTablesInSecs = delay;
            return this;
        }

        Builder withStatsClass(String statsClass)
        {
            this.statsClass = statsClass;
            return this;
        }

        public Builder withUpsert()
        {
            upsert = true;
            return this;
        }

        public Builder withNullRegularColumns()
        {
            this.nullRegularColumns = true;
            return this;
        }

        public void run()
        {
            Preconditions.checkArgument(schemaBuilder != null || schemaBuilderFunc != null);
            new Tester(this).run();
        }
    }

    private void run()
    {
        qt().forAll(versions(), numSSTables())
            .checkAssert(this::run);
    }

    private Gen<CassandraVersion> versions()
    {
        return arbitrary().pick(versions);
    }

    private Gen<Integer> numSSTables()
    {
        return arbitrary().pick(numSSTables);
    }

    // CHECKSTYLE IGNORE: Long method
    private void run(CassandraVersion version, int numSSTables)
    {
        TestUtils.runTest(version, (partitioner, directory, bridge) -> {
            String keyspace = "keyspace_" + UUID.randomUUID().toString().replaceAll("-", "");
            TestSchema schema = schemaBuilder != null ? schemaBuilder.withKeyspace(keyspace).build()
                                                      : schemaBuilderFunc.apply(keyspace).build();
            schema.setCassandraVersion(version);

            // Write SSTables with random data
            Map<String, MutableLong> sum = sumFields.stream()
                                                    .collect(Collectors.toMap(Function.identity(),
                                                                              ignore -> new MutableLong()));
            Map<String, TestSchema.TestRow> rows = new HashMap<>(numRandomRows);
            IntStream.range(0, numSSTables).forEach(ssTable -> schema.writeSSTable(directory, bridge, partitioner, upsert, writer -> {
                IntStream.range(0, numRandomRows).forEach(row -> {
                    TestSchema.TestRow testRow;
                    do
                    {
                        testRow = schema.randomRow(nullifyValueColumn);
                    }
                    while (rows.containsKey(testRow.getPrimaryHexKey()));  // Don't write duplicate rows

                    for (Consumer<TestSchema.TestRow> writeListener : writeListeners)
                    {
                        writeListener.accept(testRow);
                    }

                    for (String sumField : sumFields)
                    {
                        sum.get(sumField).add((Number) testRow.get(sumField));
                    }
                    rows.put(testRow.getPrimaryHexKey(), testRow);

                    Object[] values = testRow.allValues();
                    if (upsert)
                    {
                        rotate(values, schema.partitionKeys.size() + schema.clusteringKeys.size());
                    }
                    writer.write(values);
                });
            }));
            int sstableCount = numSSTables;

            // Write any custom SSTables e.g. overwriting existing data or tombstones
            for (Writer writer : writers)
            {
                if (sstableCount != 0)
                {
                    try
                    {
                        TimeUnit.SECONDS.sleep(delayBetweenSSTablesInSecs);
                    }
                    catch (InterruptedException exception)
                    {
                        throw new RuntimeException(exception.getMessage());
                    }
                }
                if (writer.isTombstoneWriter)
                {
                    schema.writeTombstoneSSTable(directory, bridge, partitioner, writer.consumer);
                }
                else
                {
                    schema.writeSSTable(directory, bridge, partitioner, false, writer.consumer);
                }
                sstableCount++;
            }

            if (shouldCheckNumSSTables)
            {
                assertEquals(sstableCount, TestUtils.countSSTables(directory),
                             "Number of SSTables written does not match expected");
            }

            Dataset<Row> dataset = TestUtils.openLocalDataset(bridge,
                                                              partitioner,
                                                              directory,
                                                              schema.keyspace,
                                                              schema.createStatement,
                                                              version,
                                                              schema.udts,
                                                              addLastModifiedTimestamp,
                                                              statsClass,
                                                              filterExpression,
                                                              columns);
            int rowCount = 0;
            Set<String> requiredColumns = columns != null ? new HashSet<>(Arrays.asList(columns)) : null;
            for (Row row : dataset.collectAsList())
            {
                if (requiredColumns != null)
                {
                    Set<String> actualColumns = new HashSet<>(Arrays.asList(row.schema().fieldNames()));
                    assertEquals(actualColumns, requiredColumns,
                                 "Actual Columns and Required Columns should be the same");
                }

                TestSchema.TestRow actualRow = schema.toTestRow(row, requiredColumns, getSparkSql(version));
                if (numRandomRows > 0)
                {
                    // If we wrote random data, verify values exist
                    String key = actualRow.getPrimaryHexKey();
                    assertTrue(rows.containsKey(key), "Unexpected row read in Spark");
                    assertEquals(rows.get(key).withColumns(requiredColumns), actualRow,
                                 "Row read in Spark does not match expected");
                }

                for (Consumer<TestSchema.TestRow> readListener : readListeners)
                {
                    readListener.accept(actualRow);
                }
                rowCount++;
            }
            if (expectedRowCount >= 0)
            {
                assertEquals(expectedRowCount * sstableCount, rowCount, "Number of rows read does not match expected");
            }

            // Verify numerical fields sum to expected value
            for (String sumField : sumFields)
            {
                assertEquals(sum.get(sumField).getValue().longValue(),
                             dataset.groupBy().sum(sumField).first().getLong(0),
                             "Field '" + sumField + "' does not sum to expected amount");
            }

            // Run SparkSQL checks
            for (Consumer<Dataset<Row>> check : checks)
            {
                check.accept(dataset);
            }

            if (reset != null)
            {
                reset.run();
            }
        });
    }

    public static TestSchema.TestRow newUniqueRow(TestSchema schema, Map<String, TestSchema.TestRow> rows)
    {
        return newUniqueRow(schema::randomRow, rows);
    }

    private static TestSchema.TestRow newUniqueRow(Supplier<TestSchema.TestRow> rowProvider,
                                                   Map<String, TestSchema.TestRow> rows)
    {
        TestSchema.TestRow testRow;
        do
        {
            testRow = rowProvider.get();
        }
        while (rows.containsKey(testRow.getPrimaryHexKey()));  // Don't write duplicate rows
        return testRow;
    }

    private void rotate(Object[] array, int shift)
    {
        ArrayUtils.reverse(array, 0, shift);
        ArrayUtils.reverse(array, shift, array.length);
        ArrayUtils.reverse(array, 0, array.length);
    }
}
