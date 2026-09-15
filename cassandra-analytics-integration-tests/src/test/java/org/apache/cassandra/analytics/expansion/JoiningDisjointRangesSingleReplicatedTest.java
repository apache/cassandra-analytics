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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The concurrent join in the unreplicated datacenter must receive no bulk-written rows.
 */
class JoiningDisjointRangesSingleReplicatedTest extends JoiningDisjointRangesTest
{
    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        multiDCTestInputs().forEach(arguments -> {
            QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, arguments.get());
            createTestTable(table, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration configuration = super.testClusterConfiguration();
        Map<String, Object> settings = new HashMap<>(configuration.additionalInstanceConfig);
        // As in Sidecar's single-replicated-DC test, an empty DC cannot acknowledge EACH_QUORUM.
        settings.put("progress_barrier_default_consistency_level", "QUORUM");
        return configuration.additionalInstanceConfig(settings);
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        assertThat(expectedInstanceData.get(cluster.get(11))).isNotEmpty();
        assertThat(expectedInstanceData.get(cluster.get(12))).isEmpty();
    }
}
