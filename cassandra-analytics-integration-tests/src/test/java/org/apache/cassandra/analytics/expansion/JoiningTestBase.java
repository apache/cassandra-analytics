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

package org.apache.cassandra.analytics.expansion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

abstract class JoiningTestBase extends ResiliencyTestBase
{
    Dataset<Row> df;
    Map<IInstance, Set<String>> expectedInstanceData;
    List<IInstance> newInstances;

    protected void runJoiningTestScenario(TestConsistencyLevel cl)
    {
        QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
        bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                            .save();
        // validate data right after bulk writes
        validateData(table, cl.readCL, ROW_COUNT);
        validateNodeSpecificData(table, expectedInstanceData);
    }

    @Override
    protected void beforeTestStart()
    {
        SparkSession spark = getOrCreateSparkSession();
        // Generate some artificial data for the test
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        // Generate the expected data for the new instances
        expectedInstanceData = generateExpectedInstanceData(cluster, newInstances, ROW_COUNT);
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        ClusterBuilderConfiguration configuration = testClusterConfiguration();
        UpgradeableCluster provisionedCluster = clusterBuilder(configuration, testVersion);

        newInstances = addNewInstances(provisionedCluster, configuration.newNodesPerDc, configuration.dcCount);
        TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart(), 2, TimeUnit.MINUTES);
        newInstances.forEach(instance -> ClusterUtils.awaitRingState(instance, instance, "Joining"));

        return provisionedCluster;
    }

    protected void completeTransitionsAndValidateWrites(CountDownLatch transitionalStateEnd,
                                                        Stream<Arguments> testInputs,
                                                        boolean failureExpected)
    {
        long count = transitionalStateEnd.getCount();
        for (int i = 0; i < count; i++)
        {
            transitionalStateEnd.countDown();
        }

        testInputs.forEach(arguments -> {
            TestConsistencyLevel cl = (TestConsistencyLevel) arguments.get()[0];

            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
            validateData(tableName, cl.readCL, ROW_COUNT);
            validateNodeSpecificData(tableName, expectedInstanceData);
        });

        // For tests that involve JOIN failures, we make a best-effort attempt to check if the node join has failed
        // by checking if the node has either left the ring or is still in JOINING state, but not NORMAL
        if (failureExpected)
        {
            for (IInstance joiningNode : newInstances)
            {
                Optional<ClusterUtils.RingInstanceDetails> joiningNodeDetails =
                getMatchingInstanceFromRing(cluster.get(1), joiningNode);
                joiningNodeDetails.ifPresent(ringInstanceDetails -> assertThat(ringInstanceDetails.getState())
                                                                    .isNotEqualTo("Normal"));
            }
        }
    }

    /**
     * @return a latch to wait before the cluster provisioning is complete
     */
    protected abstract CountDownLatch transitioningStateStart();

    /**
     * @return the configuration for the test cluster
     */
    protected abstract ClusterBuilderConfiguration testClusterConfiguration();

    private static List<IInstance> addNewInstances(UpgradeableCluster cluster, int newNodesPerDc, int numDcs)
    {
        List<IInstance> newInstances = new ArrayList<>();
        // Go over new nodes and add them once for each DC
        for (int i = 0; i < newNodesPerDc; i++)
        {
            int dcNodeIdx = 1; // Use node 2's DC
            for (int dc = 1; dc <= numDcs; dc++)
            {
                IInstance dcNode = cluster.get(dcNodeIdx++);
                IInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                 dcNode.config().localDatacenter(),
                                                                 dcNode.config().localRack(),
                                                                 inst -> {
                                                                     inst.set("auto_bootstrap", true);
                                                                     inst.with(Feature.GOSSIP,
                                                                               Feature.JMX,
                                                                               Feature.NATIVE_PROTOCOL);
                                                                 });
                new Thread(() -> newInstance.startup(cluster)).start();
                newInstances.add(newInstance);
            }
        }
        return newInstances;
    }

    Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IInstance seed,
                                                                           IInstance instance)
    {
        String ipAddress = instance.broadcastAddress().getAddress().getHostAddress();
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(i -> i.getAddress().equals(ipAddress))
                           .findFirst();
    }
}
