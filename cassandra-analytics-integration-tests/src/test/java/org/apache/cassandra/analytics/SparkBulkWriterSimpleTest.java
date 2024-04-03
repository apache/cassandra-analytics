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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.JvmDTestSharedClassesPredicate;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

class SparkBulkWriterSimpleTest extends SharedClusterSparkIntegrationTestBase
{
    static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (id BIGINT PRIMARY KEY, course BLOB, marks BIGINT)";

    @ParameterizedTest
    @MethodSource("options")
    void runSampleJob(Integer ttl, Long timestamp)
    {
        Map<String, String> writerOptions = new HashMap<>();

        boolean addTTLColumn = ttl != null;
        boolean addTimestampColumn = timestamp != null;
        if (addTTLColumn)
        {
            writerOptions.put(WriterOptions.TTL.name(), "ttl");
        }

        if (addTimestampColumn)
        {
            writerOptions.put(WriterOptions.TIMESTAMP.name(), "timestamp");
        }

        IntegrationTestJob.builder((recordNum) -> generateCourse(recordNum, ttl, timestamp),
                                   getWriteSchema(addTTLColumn, addTimestampColumn))
                          .withSidecarPort(server.actualPort())
                          .withExtraWriterOptions(writerOptions)
                          .withPostWriteDatasetModifier(writeToReadDfFunc(addTTLColumn, addTimestampColumn))
                          .run();
    }

    static Stream<Arguments> options()
    {
        return Stream.of(
        Arguments.of(null, null),
        Arguments.of(1000, null),
        Arguments.of(null, 1432815430948567L),
        Arguments.of(1000, 1432815430948567L)
        );
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(new QualifiedName(TEST_KEYSPACE, "test"), CREATE_TABLE_STATEMENT);
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(), Semver.SemverType.LOOSE));

        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(3)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDCs(1)
                          .withDataDirCount(1)
                          .withSharedClasses(JvmDTestSharedClassesPredicate.INSTANCE)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(3, clusterBuilder.getTokenCount());
        clusterBuilder.withTokenSupplier(tokenSupplier);
        return clusterBuilder.start();
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
