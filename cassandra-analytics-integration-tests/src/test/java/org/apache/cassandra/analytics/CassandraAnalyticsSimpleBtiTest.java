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

package org.apache.cassandra.analytics;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;

import static org.assertj.core.api.Assumptions.assumeThat;


/**
 * A simple test that runs a sample read/write Cassandra Analytics job using BTI format SSTable.
 */
class CassandraAnalyticsSimpleBtiTest extends CassandraAnalyticsSimpleTest
{
    static
    {
        System.setProperty("cassandra.analytics.bridges.sstable_format", "bti");
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        String version = TestUtils.getDTestClusterVersion().getValue();
        assumeThat(CassandraVersion.fromVersion(version)
                                   .orElseThrow()
                                   .supportedSSTableFormats())
        .as("BTI SSTable format is not supported in %s", version)
        .contains("bti");
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration();
        Map<String, Object> additionalConf = new HashMap<>(conf.additionalInstanceConfig);
        additionalConf.put("sstable", Map.of("selected_format", "bti"));
        conf.additionalInstanceConfig(additionalConf);
        return conf;
    }
}
