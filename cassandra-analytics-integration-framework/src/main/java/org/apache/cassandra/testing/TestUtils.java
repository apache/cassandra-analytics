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

package org.apache.cassandra.testing;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.sidecar.testing.QualifiedName;

/**
 * Helper class for integration testing functionality
 */
public final class TestUtils
{
    public static final String TEST_KEYSPACE = "spark_test";
    public static final String TEST_TABLE_PREFIX = "testtable";
    private static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);

    //    public static final int ROW_COUNT = 10_000;
    public static final int ROW_COUNT = 1_000;

    // Replication factor configurations used for tests
    public static final Map<String, Integer> DC1_RF1 = Collections.singletonMap("datacenter1", 1);
    public static final Map<String, Integer> DC1_RF3 = Collections.singletonMap("datacenter1", 3);
    public static final Map<String, Integer> DC1_RF3_DC2_RF3 = ImmutableMap.of("datacenter1", 3,
                                                                               "datacenter2", 3);

    public static final String CREATE_TEST_TABLE_STATEMENT =
    "CREATE TABLE IF NOT EXISTS %s (id int, course text, marks int, PRIMARY KEY (id));";

    private TestUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static QualifiedName uniqueTestTableFullName(String keyspace)
    {
        return uniqueTestTableFullName(keyspace, TEST_TABLE_PREFIX);
    }

    public static QualifiedName uniqueTestTableFullName(String keyspace, String testTablePrefix)
    {
        return new QualifiedName(keyspace, testTablePrefix + TEST_TABLE_ID.getAndIncrement());
    }
}
