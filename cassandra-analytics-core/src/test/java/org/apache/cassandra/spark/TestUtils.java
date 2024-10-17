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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.core.Gen;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public final class TestUtils extends CommonTestUtils
{
    private static class Holder
    {
        private static final SparkSession SPARK_SESSION = createSession();

        static SparkSession createSession()
        {
            return SparkSession.builder()
                               .appName("Java Test")
                               .config("spark.master", "local")
                               // Spark is not case-sensitive by default, but we want to make it case-sensitive for
                               // the quoted identifiers tests where we test mixed case
                               .config("spark.sql.caseSensitive", "True")
                               .getOrCreate();
        }
    }

    private static SparkSession session()
    {
        return Holder.SPARK_SESSION;
    }

    private TestUtils()
    {
        super();
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static long countSSTables(Path directory) throws IOException
    {
        return getFileType(directory, FileType.DATA).count();
    }

    public static Path getFirstFileType(Path directory, FileType fileType) throws IOException
    {
        return getFileType(directory, fileType).findFirst().orElseThrow(() ->
                new IllegalStateException(String.format("Could not find %s file", fileType.getFileSuffix())));
    }

    public static Stream<Path> getFileType(Path directory, FileType fileType) throws IOException
    {
        return Files.list(directory)
                    .filter(path -> path.getFileName().toString().endsWith("-" + fileType.getFileSuffix()));
    }

    /**
     * Run test for all supported Cassandra versions
     *
     * @param test unit test
     */
    public static void runTest(TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners(), TestUtils.bridges())
            .checkAssert((partitioner, bridge) -> TestUtils.runTest(partitioner, bridge, test));
    }

    public static void runTest(CassandraVersion version, TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert(partitioner -> TestUtils.runTest(partitioner, version, test));
    }

    public static void runTest(Partitioner partitioner, CassandraVersion version, TestRunnable test)
    {
        runTest(partitioner, CassandraBridgeFactory.get(version), test);
    }

    /**
     * Create tmp directory and clean up after test
     *
     * @param bridge cassandra bridge
     * @param test   unit test
     */
    public static void runTest(Partitioner partitioner, CassandraBridge bridge, TestRunnable test)
    {
        Path directory = null;
        try
        {
            directory = Files.createTempDirectory(UUID.randomUUID().toString());
            test.run(partitioner, directory, bridge);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
        finally
        {
            if (directory != null)
            {
                try
                {
                    FileUtils.deleteDirectory(directory.toFile());
                }
                catch (IOException ignore)
                {
                }
            }
        }
    }

    // CHECKSTYLE IGNORE: Method with many parameters
    static Dataset<Row> openLocalPartitionSizeSource(CassandraBridge bridge,
                                                     Partitioner partitioner,
                                                     Path dir,
                                                     String keyspace,
                                                     String createStmt,
                                                     CassandraVersion version,
                                                     Set<CqlField.CqlUdt> udts,
                                                     @Nullable String statsClass)
    {
        DataFrameReader frameReader = session()
                                      .read()
                                      .format("org.apache.cassandra.spark.sparksql.LocalPartitionSizeSource")
                                      .option("keyspace", keyspace)
                                      .option("createStmt", createStmt)
                                      .option("dirs", dir.toAbsolutePath().toString())
                                      .option("version", version.toString())
                                      .option("useBufferingInputStream", true)  // Use in the test system to test the BufferingInputStream
                                      .option("partitioner", partitioner.name())
                                      .option("udts", udts.stream().map(f -> f.createStatement(bridge.cassandraTypes(), keyspace))
                                                          .collect(Collectors.joining("\n")));
        if (statsClass != null)
        {
            frameReader = frameReader.option("statsClass", statsClass);
        }
        return frameReader.load();
    }

    public static Dataset<Row> read(Path path, StructType schema)
    {
        return session()
               .read()
               .format("parquet")
               .option("path", path.toString())
               .schema(schema)
               .load();
    }

    // CHECKSTYLE IGNORE: Method with many parameters
    public static Dataset<Row> openLocalDataset(CassandraBridge bridge,
                                                Partitioner partitioner,
                                                Path directory,
                                                String keyspace,
                                                String createStatement,
                                                CassandraVersion version,
                                                Set<CqlField.CqlUdt> udts,
                                                boolean addLastModifiedTimestampColumn,
                                                @Nullable String statsClass,
                                                @Nullable String filterExpression,
                                                @Nullable String... columns)
    {
        DataFrameReader frameReader = session()
                                      .read()
                                      .format("org.apache.cassandra.spark.sparksql.LocalDataSource")
                                      .option("keyspace", keyspace)
                                      .option("createStmt", createStatement)
                                      .option("dirs", directory.toAbsolutePath().toString())
                                      .option("version", version.toString())
                                      .option("useBufferingInputStream", true)  // Use in the test system to test the BufferingInputStream
                                      .option("partitioner", partitioner.name())
                                      .option(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName(), addLastModifiedTimestampColumn)
                                      .option("udts", udts.stream()
                                                          .map(udt -> udt.createStatement(bridge.cassandraTypes(), keyspace))
                                                          .collect(Collectors.joining("\n")));
        if (statsClass != null)
        {
            frameReader = frameReader.option("statsClass", statsClass);
        }
        Dataset<Row> dataset = frameReader.load();
        if (filterExpression != null)
        {
            // Attach partition filter criteria
            dataset = dataset.filter(filterExpression);
        }
        if (columns != null && columns.length > 0)
        {
            // Attach column select criteria
            if (columns.length == 1)
            {
                dataset = dataset.select(columns[0]);
            }
            else
            {
                dataset = dataset.select(columns[0], Arrays.copyOfRange(columns, 1, columns.length));
            }
        }
        return dataset;
    }

    public static ReplicationFactor simpleStrategy()
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy()
    {
        return networkTopologyStrategy(ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy(Map<String, Integer> options)
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, options);
    }

    /* Quick Theories Helpers */

    public static Gen<CassandraVersion> versions()
    {
        return arbitrary().pick(CassandraVersion.implementedVersions());
    }

    public static Gen<CassandraBridge> bridges()
    {
        return arbitrary().pick(testableVersions().stream()
                                                  .map(CassandraBridgeFactory::get)
                                                  .collect(Collectors.toList()));
    }

    public static List<CassandraVersion> testableVersions()
    {
        return ImmutableList.copyOf(CassandraVersion.implementedVersions());
    }

    public static Gen<CqlField.SortOrder> sortOrder()
    {
        return arbitrary().enumValues(CqlField.SortOrder.class);
    }

    public static Gen<CassandraVersion> tombstoneVersions()
    {
        return arbitrary().pick(tombstoneTestableVersions());
    }

    public static List<CassandraVersion> tombstoneTestableVersions()
    {
        // Tombstone SSTable writing and SSTable-to-JSON conversion are not implemented for Cassandra version 3.0
        return ImmutableList.of(CassandraVersion.FOURZERO);
    }

    public static Gen<Partitioner> partitioners()
    {
        return arbitrary().enumValues(Partitioner.class);
    }

    public static CassandraRing createRing(Partitioner partitioner, int numInstances)
    {
        return createRing(partitioner, ImmutableMap.of("DC1", numInstances));
    }

    public static CassandraRing createRing(Partitioner partitioner, Map<String, Integer> numInstances)
    {
        Collection<CassandraInstance> instances = numInstances.entrySet().stream()
                .map(dataCenter -> TestUtils.createInstances(partitioner, dataCenter.getValue(), dataCenter.getKey()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Map<String, Integer> dataCenters = numInstances.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, dataCenter -> Math.min(dataCenter.getValue(), 3)));
        return new CassandraRing(partitioner, "test", new ReplicationFactor(
                ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, dataCenters), instances);
    }

    public static Collection<CassandraInstance> createInstances(Partitioner partitioner,
                                                                int numInstances,
                                                                String dataCenter)
    {
        Preconditions.checkArgument(numInstances > 0, "NumInstances must be greater than zero");
        BigInteger split = partitioner.maxToken()
                                      .subtract(partitioner.minToken())
                                      .divide(BigInteger.valueOf(numInstances));
        Collection<CassandraInstance> instances = new ArrayList<>(numInstances);
        BigInteger token = partitioner.minToken();
        for (int instance = 0; instance < numInstances; instance++)
        {
            instances.add(new CassandraInstance(token.toString(), "local-i" + instance, dataCenter));
            token = token.add(split);
            assertTrue(token.compareTo(partitioner.maxToken()) <= 0);
        }
        return instances;
    }

    public static Set<String> getKeys(List<List<String>> values)
    {
        Set<String> filterKeys = new HashSet<>();
        FilterUtils.cartesianProduct(values).forEach(keys -> {
            String compositeKey = String.join(":", keys);
            filterKeys.add(compositeKey);
        });
        return filterKeys;
    }

    public static String randomLowEntropyString()
    {
        return new String(randomLowEntropyData(), StandardCharsets.UTF_8);
    }

    public static byte[] randomLowEntropyData()
    {
        return randomLowEntropyData(RandomUtils.randomPositiveInt(16384 - 512) + 512);
    }

    public static byte[] randomLowEntropyData(int size)
    {
        return randomLowEntropyData("Hello world!", size);
    }

    public static byte[] randomLowEntropyData(String str, int size)
    {
        return StringUtils.repeat(str, size / str.length() + 1)
                          .substring(0, size)
                          .getBytes(StandardCharsets.UTF_8);
    }

    public static Range<BigInteger> range(long start, long end)
    {
        return range(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    public static Range<BigInteger> range(BigInteger start, BigInteger end)
    {
        return Range.openClosed(start, end);
    }
}
