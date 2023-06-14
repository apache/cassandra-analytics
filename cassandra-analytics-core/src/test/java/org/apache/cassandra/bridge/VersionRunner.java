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
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Named;

/**
 * Shared version provider for all defined versions of Cassandra
 */
public final class VersionRunner
{
    private VersionRunner()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static Collection<CassandraVersion> versions()
    {
        return Arrays.stream(CassandraVersion.implementedVersions())
                     .collect(Collectors.toList());
    }

    public static List<Named<CassandraBridge>> bridges()
    {
        return versions().stream().map(version -> Named.of(version.toString(), CassandraBridgeFactory.get(version)))
                                            .collect(Collectors.toList());
    }
}
