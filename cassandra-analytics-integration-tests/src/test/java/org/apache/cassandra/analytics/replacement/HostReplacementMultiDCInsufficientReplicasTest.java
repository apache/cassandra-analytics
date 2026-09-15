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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.analytics.TopologyChangeBBUtils;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Validate failed write operation when host replacement fails resulting in insufficient nodes. This is simulated by
 * bringing down a node in addition to the replacement failure resulting in too few replicas to satisfy the
 * replication factor requirements.
 */
class HostReplacementMultiDCInsufficientReplicasTest extends HostReplacementTestBase
{
    static final QualifiedName QUALIFIED_NAME = TestUtils.uniqueTestTableFullName(TEST_KEYSPACE);

    @Test
    void nodeReplacementFailureMultiDCInsufficientNodes()
    {
        DataFrameWriter<Row> dfWriter = bulkWriterDataFrameWriter(df, QUALIFIED_NAME)
                                      .option(WriterOptions.BULK_WRITER_CL.name(), EACH_QUORUM.name());

        sparkTestUtils.assertExpectedBulkWriteFailure(EACH_QUORUM.name(), dfWriter);
    }

    @Override
    protected void beforeClusterShutdown()
    {
        BBHelperNodeReplacementMultiDCInsufficientReplicas.transitionalStateEnd.countDown();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        createTestTable(QUALIFIED_NAME, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(3)
                              .newNodesPerDc(1)
                              .dcCount(2)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperNodeReplacementMultiDCInsufficientReplicas::install);
    }

    @Override
    protected int additionalNodesToStop()
    {
        return 2;
    }

    @Override
    protected CountDownLatch nodeStart()
    {
        return BBHelperNodeReplacementMultiDCInsufficientReplicas.nodeStart;
    }

    /**
     * ByteBuddy helper for multi DC node replacement failure resulting in insufficient nodes
     */
    public static class BBHelperNodeReplacementMultiDCInsufficientReplicas
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
                TopologyChangeBBUtils.installBootstrap(cl, BBHelperNodeReplacementMultiDCInsufficientReplicas.class);
            }
        }


        public static boolean bootstrap(@SuperCall Callable<Boolean> orig) throws Exception
        {
            orig.call();
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transitionalStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitionalStateEnd, 2, TimeUnit.MINUTES);
            throw new UnsupportedOperationException("Simulated failure");
        }
    }
}
