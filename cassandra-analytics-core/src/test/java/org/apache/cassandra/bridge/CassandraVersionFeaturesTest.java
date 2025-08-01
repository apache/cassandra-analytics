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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CassandraVersionFeaturesTest
{
    @Test
    public void testCassandraVersions()
    {
        testCassandraVersion("1.2.3", 12, 3, "");
        testCassandraVersion("1.2.3.4", 12, 3, "4");

        testCassandraVersion("cassandra-2.0.14-v3", 20, 14, "-v3");
        testCassandraVersion("cassandra-1.2.11-v1", 12, 11, "-v1");
        testCassandraVersion("cassandra-1.2.11.2-tag", 12, 11, "2");
        testCassandraVersion("cassandra-4.0-SNAPSHOT", 40, 0, "SNAPSHOT");
        testCassandraVersion("cassandra-2.0.9-loadtest-SNAPSHOT", 20, 9, "-loadtest-SNAPSHOT");

        testCassandraVersion("qwerty-cassandra-1.2.11-v1", 12, 11, "-v1");
        testCassandraVersion("qwerty-cassandra-1.2.11.2-tag", 12, 11, "2");
        testCassandraVersion("qwerty-cassandra-4.0-SNAPSHOT", 40, 0, "SNAPSHOT");
    }

    @Test()
    public void testInvalidInput()
    {
        assertThatThrownBy(() ->
            CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion("qwerty")
        ).isInstanceOf(RuntimeException.class);
    }

    private static void testCassandraVersion(String version, int major, int minor, String suffix)
    {
        CassandraVersionFeatures features = CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version);

        assertThat(features.getMajorVersion()).as("Wrong major version for " + version + ",").isEqualTo(major);
        assertThat(features.getMinorVersion()).as("Wrong minor version for " + version + ",").isEqualTo(minor);
        assertThat(features.getSuffix()).as("Wrong version suffix for " + version + ",").isEqualTo(suffix);
        assertThat(features.getRawVersionString()).isEqualTo(version);
    }
}
