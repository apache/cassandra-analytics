/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import o.a.c.analytics.sidecar.shaded.testing.adapters.base.StorageJmxOperations;
import o.a.c.analytics.sidecar.shaded.testing.common.JmxClient;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.DecoratedKey;
import org.apache.cassandra.spark.bulkwriter.Tokenizer;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import static junit.framework.TestCase.assertTrue;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for resiliency tests. Contains helper methods for data generation and validation
 */
public abstract class ResiliencyTestBase extends IntegrationTestBase
{
    public static final int rowCount = 1000;
    protected static final String retrieveRows = "select * from " + TEST_KEYSPACE + ".%s";
    private static final Logger LOGGER = LoggerFactory.getLogger(ResiliencyTestBase.class);
    private static final String createTableStmt = "create table if not exists %s (id int, course text, marks int, primary key (id));";

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final AtomicReference<byte[]> errorOutput = new AtomicReference<>();
    private final AtomicReference<byte[]> outputBytes = new AtomicReference<>();


    public QualifiedName initializeSchema()
    {
        return initializeSchema(ImmutableMap.of("datacenter1", 1));
    }

    public QualifiedName initializeSchema(Map<String, Integer> rf)
    {
        createTestKeyspace(TEST_KEYSPACE, rf);
        return createTestTable(TEST_KEYSPACE, createTableStmt);
    }

    public Set<String> getDataForRange(Range<BigInteger> range)
    {
        // Iterate through all data entries; filter only entries that belong to range; convert to strings
        return generateExpectedData().stream()
                                     .filter(t -> range.contains(t._1().getToken()))
                                     .map(t -> t._2()[0] + ":" + t._2()[1] + ":" + t._2()[2])
                                     .collect(Collectors.toSet());
    }

    public List<Tuple2<DecoratedKey, Object[]>> generateExpectedData()
    {
        // "create table if not exists %s (id int, course text, marks int, primary key (id));";
        List<ColumnType<?>> columnTypes = Arrays.asList(ColumnTypes.INT);
        Tokenizer tokenizer = new Tokenizer(Arrays.asList(0),
                                            Arrays.asList("id"),
                                            columnTypes,
                                            true
        );
        return IntStream.range(0, rowCount).mapToObj(recordNum -> {
            Object[] columns = new Object[]
                               {
                               recordNum, "course" + recordNum, recordNum
                               };
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        }).collect(Collectors.toList());
    }

    public Map<IUpgradeableInstance, Set<String>> getInstanceData(List<IUpgradeableInstance> instances,
                                                                  boolean isPending)
    {

        return instances.stream().collect(Collectors.toMap(Function.identity(),
                                                           i -> filterTokenRangeData(getRangesForInstance(i, isPending))));
    }

    public Set<String> filterTokenRangeData(List<Range<BigInteger>> ranges)
    {
        return ranges.stream()
                     .map(this::getDataForRange)
                     .flatMap(Collection::stream)
                     .collect(Collectors.toSet());
    }

    /**
     * Returns the expected set of rows as strings for each instance in the cluster
     */
    public Map<IUpgradeableInstance, Set<String>> generateExpectedInstanceData(UpgradeableCluster cluster,
                                                                               List<IUpgradeableInstance> pendingNodes)
    {
        List<IUpgradeableInstance> instances = cluster.stream().collect(Collectors.toList());
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData = getInstanceData(instances, false);
        // Use pending ranges to get data for each transitioning instance
        Map<IUpgradeableInstance, Set<String>> transitioningInstanceData = getInstanceData(pendingNodes, true);
        expectedInstanceData.putAll(transitioningInstanceData.entrySet()
                                                             .stream()
                                                             .filter(e -> !e.getValue().isEmpty())
                                                             .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                       Map.Entry::getValue)));
        return expectedInstanceData;
    }

    public void validateData(String tableName, ConsistencyLevel cl)
    {
        String query = String.format(retrieveRows, tableName);
        try
        {
            SimpleQueryResult resultSet = sidecarTestContext.cluster().get(1).coordinator()
                                                            .executeWithResult(query, mapConsistencyLevel(cl));
            Set<String> rows = new HashSet<>();
            for (SimpleQueryResult it = resultSet; it.hasNext();)
            {
                Row row = it.next();
                if (row.get("id") == null || row.get("course") == null || row.get("marks") == null)
                {
                    throw new RuntimeException("Unrecognized row in table");
                }

                int id = row.getInteger("id");
                String course = row.getString("course");
                int marks = row.getInteger("marks");
                rows.add(id + ":" + course + ":" + marks);
            }
            for (int i = 0; i < rowCount; i++)
            {
                String expectedRow = i + ":course" + i + ":" + i;
                rows.remove(expectedRow);
            }
            assertTrue(rows.isEmpty());
        }
        catch (Exception ex)
        {
            logger.error("Validation Query failed", ex);
            throw ex;
        }
    }

    public void validateNodeSpecificData(QualifiedName table,
                                         Map<IUpgradeableInstance, Set<String>> expectedInstanceData)
    {
        validateNodeSpecificData(table, expectedInstanceData, true);
    }

    public void validateNodeSpecificData(QualifiedName table,
                                         Map<IUpgradeableInstance, Set<String>> expectedInstanceData,
                                         boolean hasNewNodes)
    {
        for (IUpgradeableInstance instance : expectedInstanceData.keySet())
        {
            SimpleQueryResult qr = instance.executeInternalWithResult(String.format(retrieveRows, table.table()));
            Set<String> rows = new HashSet<>();
            while (qr.hasNext())
            {
                org.apache.cassandra.distributed.api.Row row = qr.next();
                int id = row.getInteger("id");
                String course = row.getString("course");
                int marks = row.getInteger("marks");
                rows.add(id + ":" + course + ":" + marks);
            }

            if (hasNewNodes)
            {
                assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedInstanceData.get(instance));
            }
            else
            {
                assertThat(rows).containsAll(expectedInstanceData.get(instance));
            }
        }
    }

    public void closeStream(Closeable... closeables)
    {
        for (Closeable closeable : closeables)
        {
            try
            {
                if (closeable != null)
                {
                    closeable.close();
                }
            }
            catch (IOException e)
            {
                LOGGER.error("Error closing " + closeable.toString(), e);
            }
        }
    }

    private static String getCleanedClasspath()
    {
        String classpath = System.getProperty("java.class.path");
        Pattern pattern = Pattern.compile(":?[^:]*/dtest-\\d\\.\\d+\\.\\d+\\.jar:?");
        return pattern.matcher(classpath).replaceAll(":");
    }

    private List<Range<BigInteger>> getRangesForInstance(IUpgradeableInstance instance, boolean isPending)
    {
        IInstanceConfig config = instance.config();
        JmxClient client = JmxClient.builder()
                                    .host(config.broadcastAddress().getAddress().getHostAddress())
                                    .port(config.jmxPort())
                                    .build();
        StorageJmxOperations ss = client.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService");

        Map<List<String>, List<String>> ranges = isPending ? ss.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)
                                                           : ss.getRangeToEndpointWithPortMap(TEST_KEYSPACE);

        // filter ranges that belong to the instance
        return ranges.entrySet()
                     .stream()
                     .filter(e -> e.getValue().contains(instance.broadcastAddress().getAddress().getHostAddress()
                                                        + ":" + instance.broadcastAddress().getPort()))
                     .map(e -> unwrapRanges(e.getKey()))
                     .flatMap(Collection::stream)
                     .collect(Collectors.toList());
    }

    private List<Range<BigInteger>> unwrapRanges(List<String> range)
    {
        List<Range<BigInteger>> ranges = new ArrayList<Range<BigInteger>>();
        BigInteger start = new BigInteger(range.get(0));
        BigInteger end = new BigInteger(range.get(1));
        if (start.compareTo(end) > 0)
        {
            ranges.add(Range.openClosed(start, BigInteger.valueOf(Long.MAX_VALUE)));
            ranges.add(Range.openClosed(BigInteger.valueOf(Long.MIN_VALUE), end));
        }
        else
        {
            ranges.add(Range.openClosed(start, end));
        }
        return ranges;
    }

    private org.apache.cassandra.distributed.api.ConsistencyLevel mapConsistencyLevel(ConsistencyLevel cl)
    {
        return org.apache.cassandra.distributed.api.ConsistencyLevel.valueOf(cl.name());
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws IOException
    {
        return getMultiDCCluster(initializer, cassandraTestContext, null);
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext,
                                                   Consumer<UpgradeableCluster.Builder> additionalConfigurator)
    throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier mdcTokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                   annotation.newNodesPerDc(),
                                                                                   annotation.numDcs(),
                                                                                   1);

        int totalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        return cassandraTestContext.configureAndStartCluster(
        builder -> {
            builder.withInstanceInitializer(initializer);
            builder.withTokenSupplier(mdcTokenSupplier);
            builder.withNodeIdTopology(networkTopology(totalNodeCount,
                                                       (nodeId) -> nodeId % 2 != 0 ?
                                                                   dcAndRack("datacenter1", "rack1") :
                                                                   dcAndRack("datacenter2", "rack2")));

            if (additionalConfigurator != null)
            {
                additionalConfigurator.accept(builder);
            }
        });
    }

    protected void bulkWriteData(ConsistencyLevel writeCL, QualifiedName schema)
    throws InterruptedException
    {
        List<String> sidecarInstances = generateSidecarInstances();
        // The spark job is executed in a separate process to ensure Spark's memory is cleaned up after the run.
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        // Uncomment the line below to debug on localhost
        // command.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5151");
        command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments()
                                        .stream()
                                        // Remove any already-existing debugger agents from the arguments
                                        .filter(s -> shouldRetainSetting(s))
                                        .collect(Collectors.toList()));
        command.add("-Dspark.cassandra_analytics.request.max_connections=5");
        // Set both max and min heap sizes because otherwise
        command.add("-Xmx512m");
        command.add("-Xms512m");
        command.add("-XX:MaxDirectMemorySize=128m");
        command.add("-cp");
        String cleanedClasspath = getCleanedClasspath();
        command.add(cleanedClasspath);
        command.add(RunWriteJob.class.getName());
        command.add(String.join(",", sidecarInstances));
        command.add(schema.keyspace());
        command.add(schema.table());
        command.add(writeCL.name());
        command.add(String.valueOf(server.actualPort()));
        command.add(String.valueOf(rowCount));
        try
        {
            ProcessBuilder builder = new ProcessBuilder(command);
            LOGGER.info("Running: {}", String.join("\n", command));
            Process process = builder.start();
            CountDownLatch finishLatch = startReadOutputThreads(process, String.join(" ", command));
            boolean completed = process.waitFor(10, TimeUnit.MINUTES);
            if (!completed)
            {
                process.destroyForcibly();
                // The process doesn't necessarily finish right after destroyForcibly, but will exit.
                // Give it an extra minute to complete, which should really only take seconds.
                process.waitFor(1, TimeUnit.MINUTES);
                String errorMessage = "Spark job failed to complete after 10 minutes and was killed. " +
                                      "Check log for 'SPARK STDOUT/SPARK STDERR' for details";
                // process.exitValue throws if the process is still running - be defensive.
                int exitCode;
                if (process.isAlive())
                {
                    exitCode = -1;
                }
                else
                {
                    exitCode = process.exitValue();
                    finishLatch.await(1, TimeUnit.MINUTES);
                }
                logSparkOutputAndThrow(errorMessage, command, exitCode);
                throw new RuntimeException(errorMessage);
            }
            // Make sure the Java threads reading output have completed
            boolean finishedReading = finishLatch.await(10, TimeUnit.MINUTES);
            int exitCode = process.exitValue();
            if (exitCode != 0)
            {
                logSparkOutputAndThrow("Failed to run spark command - please see output above for more information",
                                       command, exitCode);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to run spark job", e);
        }
    }

    private void logSparkOutputAndThrow(String errorMessage, List<String> command, int exitCode)
    {
        String stdout = new String(outputBytes.get());
        String stdErr = new String(errorOutput.get());
        LOGGER.error("Spark STDOUT:\n*****{}\n*****", stdout);
        LOGGER.error("Spark STDERR:\n*****{}\n*****", stdErr);
        LOGGER.error(errorMessage);
        throw new SparkJobFailedException(errorMessage,
                                          command,
                                          exitCode,
                                          stdout,
                                          stdErr);
    }

    @NotNull
    public QualifiedName createAndWaitForKeyspaceAndTable()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;

        ImmutableMap<String, Integer> rf;
        if (annotation.numDcs() > 1 && annotation.useCrossDcKeyspace())
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF, "datacenter2", DEFAULT_RF);
        }
        else
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF);
        }
        QualifiedName schema = initializeSchema(rf);
        waitUntilSidecarPicksUpSchemaChange(schema.keyspace());
        return schema;
    }

    private static boolean shouldRetainSetting(String s)
    {
        return !s.startsWith("-agentlib:")
               && !s.startsWith("-Xmx")
               && !s.startsWith("-Xms")
               && !s.startsWith("-Djava.security.manager");
    }

    protected List<String> generateSidecarInstances()
    {
        List<String> sidecarInstances = new ArrayList<>();
        for (IUpgradeableInstance inst: sidecarTestContext.cluster())
        {
            // For the sake of test speed, only give the Sidecar client instances that are actually
            // up. This is similar to considering them "administratively blocked" assuming they
            // are down for some kind of maintenance.
            if (!inst.isShutdown())
            {
                sidecarInstances.add(inst.config().broadcastAddress().getHostName());
            }
        }
        return sidecarInstances;
    }

    private CountDownLatch startReadOutputThreads(Process ps, String commandLine)
    {
        final CountDownLatch completeLatch = new CountDownLatch(2);
        // we run threads that copy stderr and stdout to prevent the
        // external process from blocking trying to write to a full buffer.
        executorService.execute(() -> {
            final String errorThreadName = Thread.currentThread().getName();
            try
            {
                Thread.currentThread().setName("ProcessLauncher-stderr: " + commandLine);
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                copyStream(ps.getErrorStream(), bout);
                errorOutput.set(bout.toByteArray());
            }
            catch (IOException e)
            {
                errorOutput.set(("Error copying process stderr of " + commandLine + ": " + e.toString())
                                .getBytes(Charset.defaultCharset())
                );
            }
            finally
            {
                completeLatch.countDown();
                Thread.currentThread().setName(errorThreadName);
                closeStream(ps.getErrorStream());
            }
        });

        executorService.execute(() -> {
            final String outputThreadName = Thread.currentThread().getName();
            try
            {
                Thread.currentThread().setName("ProcessLauncher-stdout: " + commandLine);
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                copyStream(ps.getInputStream(), bout);
                outputBytes.set(bout.toByteArray());
            }
            catch (Exception e)
            {
                LOGGER.error("Could not read Spark output", e);
                outputBytes.set(null);
            }
            finally
            {
                completeLatch.countDown();
                Thread.currentThread().setName(outputThreadName);
                closeStream(ps.getInputStream());
            }
        });
        return completeLatch;
    }

    private void copyStream(InputStream in, OutputStream out) throws IOException
    {
        int len;
        byte[] buf = new byte[1024];
        while ((len = in.read(buf)) != -1)
        {
            out.write(buf, 0, len);
        }
    }
}
