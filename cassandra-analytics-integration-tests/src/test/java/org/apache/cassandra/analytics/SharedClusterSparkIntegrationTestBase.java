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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.ResultSet;
import com.vdurmont.semver4j.Semver;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extends functionality from {@link SharedClusterIntegrationTestBase} and provides additional functionality for running
 * Spark integration tests.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
public abstract class SharedClusterSparkIntegrationTestBase extends SharedClusterIntegrationTestBase
{
    protected SparkConf sparkConf;
    protected SparkSession sparkSession;
    protected SparkTestUtils sparkTestUtils;
    protected CassandraBridge bridge;

    public SharedClusterSparkIntegrationTestBase()
    {
        sparkTestUtils = SparkTestUtilsProvider.utils();
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        sparkTestUtils.initialize(cluster.delegate(), dnsResolver, server.actualPort(), mtlsTestHelper);
    }

    @Override
    protected void afterClusterShutdown()
    {
        super.afterClusterShutdown();
        sparkTestUtils.tearDown();
    }

    /**
     * A preconfigured {@link DataFrameReader} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param tableName the qualified name for the Cassandra table
     * @return a {@link DataFrameReader} for Cassandra bulk reads
     */
    protected DataFrameReader bulkReaderDataFrame(QualifiedName tableName)
    {
        return sparkTestUtils.defaultBulkReaderDataFrame(getOrCreateSparkConf(),
                                                         getOrCreateSparkSession(),
                                                         tableName, Collections.emptyMap());
    }

    /**
     * A preconfigured {@link DataFrameReader} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param tableName the qualified name for the Cassandra table
     * @param additionalOptions additional options for the data frame
     * @return a {@link DataFrameReader} for Cassandra bulk reads
     */
    protected DataFrameReader bulkReaderDataFrame(QualifiedName tableName, Map<String, String> additionalOptions)
    {
        return sparkTestUtils.defaultBulkReaderDataFrame(getOrCreateSparkConf(),
                                                         getOrCreateSparkSession(),
                                                         tableName,
                                                         additionalOptions);
    }

    /**
     * A preconfigured {@link DataFrameWriter} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param df        the source dataframe to write
     * @param tableName the qualified name for the Cassandra table
     * @return a {@link DataFrameWriter} for Cassandra bulk writes
     */
    protected DataFrameWriter<Row> bulkWriterDataFrameWriter(Dataset<Row> df, QualifiedName tableName)
    {
        return sparkTestUtils.defaultBulkWriterDataFrameWriter(df, tableName, Collections.emptyMap());
    }

    /**
     * A preconfigured {@link DataFrameWriter} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param df                the source dataframe to write
     * @param tableName         the qualified name for the Cassandra table
     * @param additionalOptions additional options for the data frame
     * @return a {@link DataFrameWriter} for Cassandra bulk writes
     */
    protected DataFrameWriter<Row> bulkWriterDataFrameWriter(Dataset<Row> df, QualifiedName tableName,
                                                             Map<String, String> additionalOptions)
    {
        return sparkTestUtils.defaultBulkWriterDataFrameWriter(df, tableName, additionalOptions);
    }

    protected SparkConf getOrCreateSparkConf()
    {
        if (sparkConf == null)
        {
            sparkConf = sparkTestUtils.defaultSparkConf();
        }
        return sparkConf;
    }

    protected SparkSession getOrCreateSparkSession()
    {
        if (sparkSession == null)
        {
            sparkSession = SparkSession
                           .builder()
                           .config(getOrCreateSparkConf())
                           .getOrCreate();
        }
        return sparkSession;
    }

    protected CassandraBridge getOrCreateBridge()
    {
        if (bridge == null)
        {
            Semver semVer = new Semver(testVersion.version(), Semver.SemverType.LOOSE);
            bridge = CassandraBridgeFactory.get(semVer.toStrict().toString());
        }
        return bridge;
    }

    public void checkSmallDataFrameEquality(Dataset<Row> expected, Dataset<Row> actual)
    {
        if (actual == null)
        {
            throw new NullPointerException("actual dataframe is null");
        }
        if (expected == null)
        {
            throw new NullPointerException("expected dataframe is null");
        }
        // Simulate `actual` having fewer rows, but all match rows in `expected`.
        // The previous implementation would consider these equal
        // actual = actual.limit(1000);
        if (!actual.exceptAll(expected).isEmpty() || !expected.exceptAll(actual).isEmpty())
        {
            throw new IllegalStateException("The content of the dataframes differs");
        }
    }

    /**
     * Asserts that every on-disk SSTable data file for the given table matches the expected SSTable format and
     * version across all running nodes. Data file names follow the pattern
     * {@code <version>-<generation>-<format>-Data.db} (e.g. {@code oa-1-big-Data.db}). The generation component is
     * matched loosely since it may be sequence- or UUID-based depending on cluster configuration.
     *
     * @param table           the table whose on-disk SSTables are inspected
     * @param format          the expected SSTable format (e.g. {@code big})
     * @param expectedVersion the expected SSTable version (e.g. {@code oa} or {@code nb})
     */
    protected void assertSSTableFormatOnDisk(QualifiedName table, String format, String expectedVersion)
    {
        String dataFileRegex = expectedVersion + "-[^-]+-" + format + "-Data\\.db";
        boolean foundDataFiles = false;
        for (int i = 1; i <= cluster.size(); i++)
        {
            IInstance instance = cluster.get(i);
            if (instance.isShutdown())
            {
                continue;
            }

            for (String fileName : findSSTableDataFiles(instance, table))
            {
                foundDataFiles = true;
                assertThat(fileName)
                .as("SSTable data file for %s on node %d should be in %s format with version %s: %s",
                    table, i, format, expectedVersion, fileName)
                .matches(dataFileRegex);
            }
        }
        assertThat(foundDataFiles)
        .as("Expected to find at least one SSTable data file for %s on a running node", table)
        .isTrue();
    }

    /**
     * Finds the names of all SSTable {@code *-Data.db} files belonging to the given table on a single node,
     * scanning every configured data directory and scoping to the table's own data subdirectory.
     */
    protected Set<String> findSSTableDataFiles(IInstance instance, QualifiedName table)
    {
        String[] dataDirs = (String[]) instance.config().getParams().get("data_file_directories");
        Set<String> dataFileNames = new HashSet<>();
        String tableDirPrefix = table.table() + "-";
        for (String dataDir : dataDirs)
        {
            Path keyspacePath = Paths.get(dataDir, table.keyspace());
            if (!Files.exists(keyspacePath))
            {
                continue;
            }

            try (Stream<Path> walkStream = Files.walk(keyspacePath))
            {
                walkStream.filter(Files::isRegularFile)
                          .filter(path -> path.getFileName().toString().endsWith("-Data.db"))
                          .filter(path -> path.getParent() != null
                                          && path.getParent().getFileName().toString().startsWith(tableDirPrefix))
                          .forEach(path -> dataFileNames.add(path.getFileName().toString()));
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to list SSTable data files for " + table, e);
            }
        }

        return dataFileNames;
    }

    public void validateWritesWithDriverResultSet(List<Row> sparkData, ResultSet driverData,
                                                  Function<com.datastax.driver.core.Row, String> driverRowFormatter)
    {
        Set<String> driverEntries = new HashSet<>();
        driverData.forEach(row -> driverEntries.add(driverRowFormatter
                .apply(row)
                // Driver Codec writes "NULL" for null value. Spark DF writes "null".
                .replace("NULL", "null")));

        // Number of entries in Cassandra must match the original datasource
        assertThat(driverEntries.size()).isEqualTo(sparkData.size());

        // remove from actual entries to make sure that the data read is the same as the data written
        Set<String> sparkEntries = sparkData.stream().map(this::formattedSparkRow)
                .collect(Collectors.toSet());
        assertThat(driverEntries).as("All entries are expected to be read from database")
                .containsExactlyInAnyOrderElementsOf(sparkEntries);
    }

    private String formattedSparkRow(Row row)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++)
        {
            maybeFormatSparkCompositeType(sb, row.get(i));
            if (i != (row.size() - 1))
            {
                sb.append(":");
            }
        }
        return sb.toString();
    }

    // Format a Spark row to look like what the toString on a UDT looks like
    // Unfortunately not _quite_ json, so we need to do this manually.
    protected void maybeFormatSparkCompositeType(StringBuilder sb, Object o)
    {
        if (o instanceof Row)
        {
            Row r = (Row) o;
            sb.append("{");
            StructField[] fields = r.schema().fields();
            for (int i = 0; i < r.size(); i++)
            {
                sb.append(maybeQuoteFieldName(fields[i]));
                sb.append(":");
                maybeFormatSparkCompositeType(sb, r.get(i));
                if (i != r.size() - 1)
                {
                    sb.append(',');
                }
            }
            sb.append("}");
        }
        else if (o instanceof Seq) // can't differentiate between scala list and set, both come here as Seq
        {
            List<?> entries = JavaConverters.seqAsJavaListConverter((Seq<?>) o).asJava();
            sb.append("{");
            for (int i = 0; i < entries.size(); i++)
            {
                maybeFormatSparkCompositeType(sb, entries.get(i));
                if (i != (entries.size() - 1))
                {
                    sb.append(',');
                }
            }
            sb.append("}");
        }
        else if (o instanceof scala.collection.Map)
        {
            Map<?, ?> map = JavaConverters.mapAsJavaMapConverter(((scala.collection.Map<?, ?>) o)).asJava();
            for (Map.Entry<?, ?> entry : map.entrySet())
            {
                sb.append("{");
                maybeFormatSparkCompositeType(sb, entry.getKey());
                sb.append(":");
                maybeFormatSparkCompositeType(sb, entry.getValue());
                sb.append("}");
            }
        }
        else if (o instanceof String)
        {
            sb.append(String.format("'%s'", o));
        }
        else
        {
            sb.append(String.format("%s", o));
        }
    }

    protected String maybeQuoteFieldName(StructField fields)
    {
        return getOrCreateBridge().maybeQuoteIdentifier(fields.name());
    }
}
