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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

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

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF2_DC2_RF2;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Integration tests that validates successful bulk writes during a host replacement operation in a multi-datacenter
 * Cassandra cluster where the replacement operation is expected to fail. Additionally, it validates that the
 * node intended to be replaced is 'Down' and the replacement node is in 'Normal' state.
 */
class HostReplacementMultiDCFailureTest extends HostReplacementTestBase
{
    static final QualifiedName QUALIFIED_NAME = uniqueTestTableFullName(TEST_KEYSPACE, LOCAL_QUORUM, LOCAL_QUORUM);

    @Test
    void nodeReplacementFailureMultiDC()
    {
        bulkWriterDataFrameWriter(df, QUALIFIED_NAME).option(WriterOptions.BULK_WRITER_CL.name(), LOCAL_QUORUM.name())
                                                     .save();
    }

    @Override
    protected void beforeClusterShutdown()
    {
        Stream<Arguments> testInputs = Stream.of(Arguments.of(TestConsistencyLevel.of(LOCAL_QUORUM, LOCAL_QUORUM)));
        completeTransitionsAndValidateWrites(BBHelperReplacementFailureMultiDC.transitionalStateEnd,
                                             testInputs,
                                             true);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF2_DC2_RF2);
        createTestTable(QUALIFIED_NAME, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .newNodesPerDc(1)
                              .dcCount(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperReplacementFailureMultiDC::install);
    }

    @Override
    protected CountDownLatch nodeStart()
    {
        return BBHelperReplacementFailureMultiDC.nodeStart;
    }

    /**
     * ByteBuddy helper for multi DC node replacement failure
     */
    public static class BBHelperReplacementFailureMultiDC
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
                               .intercept(MethodDelegation.to(BBHelperReplacementFailureMultiDC.class))
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
            throw new UnsupportedOperationException("Simulated failure");
        }
    }
}
