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

package org.apache.cassandra.analytics.shrink;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class LeavingMultiDCFailureTest extends LeavingTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("multiDCTestInputs")
    void testLeavingFailureScenario(TestConsistencyLevel cl)
    {
        QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
        bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                            .save();
        // validate data right after bulk writes
        validateData(table, cl.readCL, ROW_COUNT);
        validateNodeSpecificData(table, generateExpectedInstanceData(cluster, leavingNodes, ROW_COUNT), false);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        multiDCTestInputs().forEach(arguments -> {
            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, arguments.get());
            createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperLeavingNodesMultiDCFailure.transitionalStateEnd, multiDCTestInputs());

        // For tests that involve LEAVE failures, we validate that the leaving nodes are part of the cluster
        // check leave node are part of cluster when leave fails
        assertThat(areLeavingNodesPartOfCluster(cluster.get(1), leavingNodes)).isTrue();
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().dcCount(2)
                              .nodesPerDc(5)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperLeavingNodesMultiDCFailure::install);
    }

    @Override
    protected int leavingNodesPerDc()
    {
        return 1;
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperLeavingNodesMultiDCFailure.transitionalStateStart;
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC failure scenario
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDCFailure
    {
        static final CountDownLatch transitionalStateStart = new CountDownLatch(2);
        static final CountDownLatch transitionalStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDCFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transitionalStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulate leave failure");
        }
    }
}
