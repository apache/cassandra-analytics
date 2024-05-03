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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Range;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.DecoratedKey;
import org.apache.cassandra.spark.bulkwriter.Tokenizer;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import scala.Tuple2;

import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for resiliency tests. Contains helper methods for data generation and validation
 */
public abstract class ResiliencyTestBase extends SharedClusterSparkIntegrationTestBase
{
    public static final String QUERY_ALL_ROWS = "SELECT * FROM %s";

    public Set<String> getDataForRange(Range<BigInteger> range, int rowCount)
    {
        // Iterate through all data entries; filter only entries that belong to range; convert to strings
        return generateExpectedData(rowCount).stream()
                                             .filter(t -> range.contains(t._1().getToken()))
                                             .map(t -> t._2()[0] + ":" + t._2()[1] + ":" + t._2()[2])
                                             .collect(Collectors.toSet());
    }

    public List<Tuple2<DecoratedKey, Object[]>> generateExpectedData(int rowCount)
    {
        // "create table if not exists %s (id int, course text, marks int, primary key (id));";
        List<ColumnType<?>> columnTypes = Collections.singletonList(ColumnTypes.INT);
        Tokenizer tokenizer = new Tokenizer(Collections.singletonList(0),
                                            Collections.singletonList("id"),
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

    public Map<IInstance, Set<String>> getInstanceData(List<? extends IInstance> instances,
                                                       boolean isPending, int rowCount)
    {
        return instances.stream().collect(Collectors.toMap(Function.identity(),
                                                           i -> filterTokenRangeData(rangesForInstance(i, isPending), rowCount)));
    }

    public Set<String> filterTokenRangeData(List<Range<BigInteger>> ranges, int rowCount)
    {
        return ranges.stream()
                     .map((Range<BigInteger> range) -> getDataForRange(range, rowCount))
                     .flatMap(Collection::stream)
                     .collect(Collectors.toSet());
    }

    /**
     * Returns the expected set of rows as strings for each instance in the cluster
     */
    public Map<IInstance, Set<String>> generateExpectedInstanceData(ICluster<? extends IInstance> cluster,
                                                                    List<? extends IInstance> pendingNodes,
                                                                    int rowCount)
    {
        List<IInstance> instances = cluster.stream().collect(Collectors.toList());
        Map<IInstance, Set<String>> expectedInstanceData = getInstanceData(instances, false, rowCount);
        // Use pending ranges to get data for each transitioning instance
        Map<IInstance, Set<String>> transitioningInstanceData = getInstanceData(pendingNodes, true, rowCount);
        expectedInstanceData.putAll(transitioningInstanceData.entrySet()
                                                             .stream()
                                                             .filter(e -> !e.getValue().isEmpty())
                                                             .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                       Map.Entry::getValue)));
        return expectedInstanceData;
    }

    protected void validateData(QualifiedName table, ConsistencyLevel readCL, int rowCount)
    {
        String query = String.format(QUERY_ALL_ROWS, table);

        Set<String> actualEntries =
        Arrays.stream(cluster.coordinator(1)
                             .execute(query, mapConsistencyLevel(readCL)))
              .map((Object[] columns) -> String.format("%s:%s:%s", columns[0], columns[1], columns[2]))
              .collect(Collectors.toSet());

        assertThat(actualEntries.size()).isEqualTo(rowCount);

        IntStream.range(0, rowCount)
                 .forEach(i -> actualEntries.remove(i + ":course" + i + ":" + i));

        assertThat(actualEntries).isEmpty();
    }

    protected static QualifiedName uniqueTestTableFullName(String keyspace, Object[] arguments)
    {
        TestConsistencyLevel cl = (TestConsistencyLevel) arguments[0];
        return uniqueTestTableFullName(keyspace, cl.readCL, cl.writeCL);
    }

    protected static QualifiedName uniqueTestTableFullName(String keyspace, ConsistencyLevel readCL, ConsistencyLevel writeCL)
    {
        String tableName = String.format("r_%s__w_%s", readCL, writeCL).toLowerCase();
        return new QualifiedName(keyspace, tableName);
    }

    public void validateNodeSpecificData(QualifiedName table,
                                         Map<? extends IInstance, Set<String>> expectedInstanceData)
    {
        validateNodeSpecificData(table, expectedInstanceData, true);
    }

    public void validateNodeSpecificData(QualifiedName table,
                                         Map<? extends IInstance, Set<String>> expectedInstanceData,
                                         boolean hasNewNodes)
    {
        for (IInstance instance : expectedInstanceData.keySet())
        {
            SimpleQueryResult qr = instance.executeInternalWithResult(String.format(QUERY_ALL_ROWS, table));
            Set<String> rows = new HashSet<>();
            while (qr.hasNext())
            {
                Row row = qr.next();
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

    private List<Range<BigInteger>> rangesForInstance(IInstance instance, boolean isPending)
    {
        IInstanceConfig config = instance.config();
        Map<List<String>, List<String>> ranges = null;
        try (JmxClient client = wrapJmxClient(JmxClient.builder()
                                                       .host(config.broadcastAddress().getAddress().getHostAddress())
                                                       .port(config.jmxPort())))
        {
            SSProxy ss = client.proxy(SSProxy.class, "org.apache.cassandra.db:type=StorageService");

            ranges = isPending ? ss.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)
                               : ss.getRangeToEndpointWithPortMap(TEST_KEYSPACE);
        }
        catch (IOException exception)
        {
            logger.warn("Unable to close JMX client");
        }

        assertThat(ranges).isNotNull();
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

    public static ClusterBuilderConfiguration clusterConfig()
    {
        return new ClusterBuilderConfiguration();
    }

    public static class TestConsistencyLevel
    {
        public final ConsistencyLevel readCL;
        public final ConsistencyLevel writeCL;

        private TestConsistencyLevel(ConsistencyLevel readCL, ConsistencyLevel writeCL)
        {
            this.readCL = Objects.requireNonNull(readCL, "readCL is required");
            this.writeCL = Objects.requireNonNull(writeCL, "writeCL is required");
        }

        public static TestConsistencyLevel of(ConsistencyLevel readCL, ConsistencyLevel writeCL)
        {
            return new TestConsistencyLevel(readCL, writeCL);
        }

        @Override
        public String toString()
        {
            return "readCL=" + readCL + ", writeCL=" + writeCL;
        }
    }

    /**
     * An interface that pulls a method from the Cassandra Storage Service Proxy
     */
    public interface SSProxy
    {
        Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace);

        Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace);
    }
}
