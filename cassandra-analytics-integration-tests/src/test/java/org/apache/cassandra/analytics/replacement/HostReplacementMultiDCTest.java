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

package org.apache.cassandra.analytics.replacement;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestConsistencyLevel;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Integration tests that verify bulk writes during a host replacement operation in a multi-datacenter
 * Cassandra cluster where the replacement operation is expected to succeed
 */
class HostReplacementMultiDCTest extends HostReplacementTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("multiDCTestInputs")
    void nodeReplacementMultiDCTest(TestConsistencyLevel cl)
    {
        QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, cl.readCL, cl.writeCL);
        bulkWriterDataFrameWriter(df, table).option(WriterOptions.BULK_WRITER_CL.name(), cl.writeCL.name())
                                            .save();
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionsAndValidateWrites(BBHelperNodeReplacementMultiDC.transitionalStateEnd,
                                             multiDCTestInputs(), false);
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
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .newNodesPerDc(1)
                              .dcCount(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperNodeReplacementMultiDC::install);
    }

    @Override
    protected CountDownLatch nodeStart()
    {
        return BBHelperNodeReplacementMultiDC.nodeStart;
    }

    static Stream<Arguments> multiDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, LOCAL_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, EACH_QUORUM)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM))
        );
    }

    /**
     * ByteBuddy helper for a multi DC node replacement that succeeds
     */
    public static class BBHelperNodeReplacementMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static final CountDownLatch nodeStart = new CountDownLatch(1);
        static final CountDownLatch transitionalStateStart = new CountDownLatch(1);
        static final CountDownLatch transitionalStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 6 node cluster (3 per DC) with a replacement node
            // We intercept the bootstrap of the replacement (7th) node to validate token ranges
            if (nodeNumber == 7)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperNodeReplacementMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            return result;
        }
    }
}
