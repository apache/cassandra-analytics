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

package org.apache.cassandra.bridge;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.spark.data.CqlField;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the Gradle copy-forward that builds each bridge jar.
 * <p>
 * A bridge jar carries the compiled classes of the version below it, and a class of the version below wins
 * whenever this version's module holds no source file of the same name. The build reports nothing, so a class
 * that names its own Cassandra version keeps naming the older one. These tests load each bridge from its
 * embedded jar, the way a Spark job does, and ask every such class which version it belongs to.
 */
class BridgeVersionConsistencyTest
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#versions")
    void testBridgeComesFromItsOwnJar(CassandraVersion version)
    {
        assertThat(CassandraBridgeFactory.get(version).getVersion().jarBaseName()).isEqualTo(version.jarBaseName());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#versions")
    void testTypesReportTheBridgeVersion(CassandraVersion version)
    {
        CassandraBridge bridge = CassandraBridgeFactory.get(version);
        // Several Cassandra versions share one jar, and the classes of that jar all name the version that built it
        CassandraVersion implemented = bridge.getVersion();
        for (CqlField.NativeType type : bridge.allTypes())
        {
            assertThat(type.version()).describedAs(type.name()).isEqualTo(implemented);
        }

        // A complex type lives in its own package and so needs an override of its own
        assertThat(bridge.tuple(bridge.aInt(), bridge.text()).version()).isEqualTo(implemented);
        assertThat(bridge.list(bridge.aInt()).version()).isEqualTo(implemented);
        assertThat(bridge.set(bridge.aInt()).version()).isEqualTo(implemented);
        assertThat(bridge.map(bridge.aInt(), bridge.text()).version()).isEqualTo(implemented);
    }
}
