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

package org.apache.cassandra.spark;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionSizeTests extends VersionRunner
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#versions")
    public void testReadingPartitionSize(CassandraVersion version)
    {
        TestUtils.runTest(version, (partitioner, dir, bridge) -> {
            int numRows = Tester.DEFAULT_NUM_ROWS;
            int numCols = 25;
            TestSchema schema = TestSchema.builder()
                                                .withPartitionKey("a", bridge.text())
                                                .withClusteringKey("b", bridge.aInt())
                                                .withColumn("c", bridge.aInt())
                                                .withColumn("d", bridge.text()).build();

            Map<String, Integer> sizes = new HashMap<>(numRows);
            schema.writeSSTable(dir, bridge, partitioner, (writer) -> {
                for (int i = 0; i < numRows; i++)
                {
                    String key = UUID.randomUUID().toString();
                    int size = 0;
                    for (int j = 0; j < numCols; j++)
                    {
                        String str = TestUtils.randomLowEntropyString();
                        writer.write(key, j, i + j, str);
                        size += 4 + 4 + str.getBytes(StandardCharsets.UTF_8).length;
                    }
                    sizes.put(key, size);
                }
            });

            Dataset<Row> ds = TestUtils.openLocalPartitionSizeSource(partitioner,
                                                                           dir,
                                                                           schema.keyspace,
                                                                           schema.createStatement,
                                                                           version,
                                                                           Collections.emptySet(),
                                                                           null);
            List<Row> rows = ds.collectAsList();
            assertEquals(numRows, rows.size());
            for (Row row : rows)
            {
                String key = row.getString(0);
                long uncompressed = row.getLong(1);
                long compressed = row.getLong(2);
                assertTrue(sizes.containsKey(key));
                long len = sizes.get(key);
                assertTrue(len < uncompressed);
                assertTrue(Math.abs(uncompressed - len) < 500); // uncompressed size should be ~len size but with a fixed overhead
                assertTrue(compressed < uncompressed);
                assertTrue(compressed / (float) uncompressed < 0.1);
            }
        });
    }
}
