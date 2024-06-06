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

import com.google.common.annotations.VisibleForTesting;

/*
 * An enum that describes all possible Cassandra versions that can potentially be supported, even if the bridge is not yet implemented.
 * Customers of this library looking to implement additional bridges or replace existing ones with proprietary implementations
 * should inject/replace bridge implementation JArs embedded into this library's resources and replace this class with an identical one,
 * but with implementedVersions() and supportedVersions() modified accordingly.
 */
public enum CassandraVersion
{
    THREEZERO(30, "3.0", "three-zero"),
    FOURZERO(40, "4.0", "four-zero"),
    FOURONE(41, "4.1", "four-zero");
    private final int number;
    private final String name;
    private final String jarBaseName;  // Must match shadowJar.archiveFileName from Gradle configuration (without extension)

    CassandraVersion(int number, String name, String jarBaseName)
    {
        this.number = number;
        this.name = name;
        this.jarBaseName = jarBaseName;
    }

    public int versionNumber()
    {
        return number;
    }

    public String versionName()
    {
        return name;
    }

    public String jarBaseName()
    {
        return jarBaseName;
    }

    private static final CassandraVersion[] implementedVersions;
    private static final String[] supportedVersions;

    static
    {
        String providedVersionsOrDefault = System.getProperty("cassandra.analytics.bridges.implemented_versions",
                                                              FOURZERO.name());
        implementedVersions = Arrays.stream(providedVersionsOrDefault.split(","))
                                    .map(CassandraVersion::valueOf)
                                    .toArray(CassandraVersion[]::new);

        String providedSupportedVersionsOrDefault = System.getProperty("cassandra.analytics.bridges.supported_versions",
                                                                       "cassandra-4.0.12");
        supportedVersions = Arrays.stream(providedSupportedVersionsOrDefault.split(","))
                                  .toArray(String[]::new);
    }

    public static CassandraVersion[] implementedVersions()
    {
        return implementedVersions;
    }

    @VisibleForTesting
    public static String[] supportedVersions()
    {
        return supportedVersions;
    }
}
