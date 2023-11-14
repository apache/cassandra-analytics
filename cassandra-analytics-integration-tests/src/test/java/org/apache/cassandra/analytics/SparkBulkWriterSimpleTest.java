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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class SparkBulkWriterSimpleTest extends IntegrationTestBase
{

    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true)
    public void runSampleJob()
    {
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        String keyspace = "spark_test";
        String table = "test";
        cluster.schemaChange(
        "  CREATE KEYSPACE " + keyspace + " WITH replication = "
        + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
        + "      AND durable_writes = true;");
        cluster.schemaChange("CREATE TABLE " + keyspace + "." + table + " (\n"
                             + "          id BIGINT PRIMARY KEY,\n"
                             + "          course BLOB,\n"
                             + "          marks BIGINT\n"
                             + "     );");
        waitForKeyspaceAndTable(keyspace, table);
        Map<String, String> writerOptions = new HashMap<>();
        // A constant timestamp and TTL can be used by adding the following options to the writerOptions map
        // writerOptions.put(WriterOptions.TTL.name(), TTLOption.constant(20));
        // writerOptions.put(WriterOptions.TIMESTAMP.name(), TimestampOption.constant(System.currentTimeMillis() * 1000));
        // Then, set ttl or timestamp to non-null values.
        Integer ttl = null;
        Long timestamp = null;
        @SuppressWarnings("ConstantValue")
        boolean addTTLColumn = ttl != null;
        @SuppressWarnings("ConstantValue")
        boolean addTimestampColumn = timestamp != null;
        IntegrationTestJob.builder((recordNum) -> generateCourse(recordNum, ttl, timestamp),
                                   getWriteSchema(addTTLColumn, addTimestampColumn))
                          .withSidecarPort(server.actualPort())
                          .withExtraWriterOptions(writerOptions)
                          .withPostWriteDatasetModifier(writeToReadDfFunc(addTTLColumn, addTimestampColumn))
                          .run();
    }

    // Because the read part of the integration test job doesn't read ttl and timestamp columns, we need to remove them
    // from the Dataset after it's saved.
    private Function<Dataset<Row>, Dataset<Row>> writeToReadDfFunc(boolean addedTTLColumn, boolean addedTimestampColumn)
    {
        return (Dataset<Row> df) -> {
            if (addedTTLColumn)
            {
                df = df.drop("ttl");
            }
            if (addedTimestampColumn)
            {
                df = df.drop("timestamp");
            }
            return df;
        };
    }

    @SuppressWarnings("SameParameterValue")
    private static StructType getWriteSchema(boolean addTTLColumn, boolean addTimestampColumn)
    {
        StructType schema = new StructType()
                            .add("id", LongType, false)
                            .add("course", BinaryType, false)
                            .add("marks", LongType, false);
        if (addTTLColumn)
        {
            schema = schema.add("ttl", IntegerType, false);
        }
        if (addTimestampColumn)
        {
            schema = schema.add("timestamp", LongType, false);
        }
        return schema;
    }

    @NotNull
    @SuppressWarnings("SameParameterValue")
    private static Row generateCourse(long recordNumber, Integer ttl, Long timestamp)
    {
        String courseNameString = String.valueOf(recordNumber);
        int courseNameStringLen = courseNameString.length();
        int courseNameMultiplier = 1000 / courseNameStringLen;
        byte[] courseName = dupStringAsBytes(courseNameString, courseNameMultiplier);
        ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNumber, courseName, recordNumber));
        if (ttl != null)
        {
            values.add(ttl);
        }
        if (timestamp != null)
        {
            values.add(timestamp);
        }
        return RowFactory.create(values.toArray());
    }

    private static byte[] dupStringAsBytes(String string, Integer times)
    {
        byte[] stringBytes = string.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(stringBytes.length * times);
        for (int time = 0; time < times; time++)
        {
            buffer.put(stringBytes);
        }
        return buffer.array();
    }
}
