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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Runs parameterized tests for all defined versions of Cassandra
 */
@RunWith(Parameterized.class)
public abstract class VersionRunner
{
    protected final CassandraVersion version;
    protected final CassandraBridge bridge;

    @Parameterized.Parameters
    public static Collection<Object[]> versions()
    {
        return Arrays.stream(CassandraVersion.implementedVersions())
                     .map(version -> new Object[]{version})
                     .collect(Collectors.toList());
    }

    public VersionRunner(CassandraVersion version)
    {
        this.version = version;
        this.bridge = CassandraBridgeFactory.get(version);
    }
}
