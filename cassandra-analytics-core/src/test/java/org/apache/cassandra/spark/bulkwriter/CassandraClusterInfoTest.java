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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;

class CassandraClusterInfoTest
{
    private SparkConf sparkConf;
    private BulkSparkConf bulkSparkConf;
    private Map<String, String> defaultOptions;

    @BeforeEach
    void before()
    {
        sparkConf = new SparkConf();
        defaultOptions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        defaultOptions.put(WriterOptions.SIDECAR_INSTANCES.name(), "127.0.0.1");
        defaultOptions.put(WriterOptions.KEYSPACE.name(), "ks");
        defaultOptions.put(WriterOptions.TABLE.name(), "table");
        defaultOptions.put(WriterOptions.KEYSTORE_PASSWORD.name(), "dummy_password");
        defaultOptions.put(WriterOptions.KEYSTORE_PATH.name(), "dummy_path");
        bulkSparkConf = new BulkSparkConf(sparkConf, defaultOptions);
    }

    @Test
    void testCalculateClusterWriterAvailability()
    {
        CassandraClusterInfo info = new CassandraClusterInfo(bulkSparkConf);
        Map<RingInstance, WriteAvailability> availabilities = info.clusterWriteAvailability();
        System.out.println(availabilities);
    }
}
