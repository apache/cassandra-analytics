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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestConsistencyLevel;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;
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
    private final List<FutureTask<Void>> joiningStartups = new ArrayList<>();

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
        super.beforeTestStart();
        SparkSession spark = getOrCreateSparkSession();
        // Generate some artificial data for the test
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        // Generate the expected data for the new instances
        expectedInstanceData = generateExpectedInstanceData(cluster, newInstances, ROW_COUNT);
    }

    @Override
    protected void afterClusterProvisioned()
    {
        if (requiresConcurrentTopologyChanges())
        {
            startTopologyChange();
        }
    }

    @Override
    protected void afterSchemaInitialized()
    {
        if (!requiresConcurrentTopologyChanges())
        {
            startTopologyChange();
        }
    }

    private void startTopologyChange()
    {
        prepareTopologyChange();
        newInstances = new ArrayList<>();
        ClusterBuilderConfiguration configuration = testClusterConfiguration();
        int nodesPerDc = requiresConcurrentTopologyChanges() ? configuration.newNodesPerDc : 1;
        int datacenters = joiningDatacenters();
        for (int i = 0; i < nodesPerDc; i++)
        {
            for (int dc = 1; dc <= datacenters; dc++)
            {
                newInstances.add(addJoiningInstance(cluster, cluster.get(dc)));
            }
        }
        TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateStart(), 2, TimeUnit.MINUTES);
        newInstances.forEach(instance -> cluster.awaitRingState(instance, instance, "Joining"));
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

        joiningStartups.forEach(startup -> awaitTopologyChange(startup, failureExpected));
        if (!failureExpected)
        {
            newInstances.forEach(instance -> cluster.awaitRingState(cluster.get(1), instance, "Normal"));
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

    protected int joiningDatacenters()
    {
        return requiresConcurrentTopologyChanges() ? testClusterConfiguration().dcCount : 1;
    }

    private IInstance addJoiningInstance(IClusterExtension<? extends IInstance> cluster, IInstance seed)
    {
        IInstance joining = cluster.addInstance(seed.config().localDatacenter(),
                                               seed.config().localRack(),
                                               config -> {
                                                   config.set("auto_bootstrap", true);
                                                   config.set("dtest.api.startup.failure_as_shutdown", false);
                                                   config.with(Feature.GOSSIP, Feature.JMX, Feature.NATIVE_PROTOCOL);
                                               });
        FutureTask<Void> joiningStartup = new FutureTask<>(() -> joining.startup(cluster.delegate()), null);
        joiningStartups.add(joiningStartup);
        new Thread(joiningStartup, "joining-node-startup").start();
        return joining;
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
