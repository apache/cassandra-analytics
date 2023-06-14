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

package org.apache.cassandra.spark.data;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;

/**
 * Run tests Parameterized for multiple versions of Cassandra
 */
public class VersionRunner
{
    protected VersionRunner()
    {
    }

    public static Collection<CassandraVersion> versions()
    {
        return Arrays.stream(CassandraVersion.implementedVersions())
                     .collect(Collectors.toList());
    }

    public static Collection<CassandraBridge> bridges()
    {
        return versions().stream().map(CassandraBridgeFactory::get)
                         .collect(Collectors.toList());
    }
}

