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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SortedSSTableWriterTest
{
    private static String previousMbeanState;

    public static Iterable<Object[]> data()
    {
        return Arrays.stream(CassandraVersion.supportedVersions())
                     .map(version -> new Object[]{version })
                     .collect(Collectors.toList());
    }

    @NotNull
    private final TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 12);

    @BeforeAll
    public static void setProps()
    {
        previousMbeanState = System.getProperty("org.apache.cassandra.disable_mbean_registration");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    @AfterAll
    public static void restoreProps()
    {
        if (previousMbeanState != null)
        {
            System.setProperty("org.apache.cassandra.disable_mbean_registration", previousMbeanState);
        }
        else
        {
            System.clearProperty("org.apache.cassandra.disable_mbean_registration");
        }
    }

    @TempDir
    private Path tmpDir;


    @ParameterizedTest
    @MethodSource("data")
    public void canCreateWriterForVersion(String version) throws IOException
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(tokenRangeMapping, version, ConsistencyLevel.CL.LOCAL_QUORUM);
        SortedSSTableWriter tw = new SortedSSTableWriter(writerContext, tmpDir, new XXHash32DigestAlgorithm(), 1);
        tw.addRow(BigInteger.ONE, ImmutableMap.of("id", 1, "date", 1, "course", "foo", "marks", 1));
        tw.close(writerContext);
        Set<Path> dataFilePaths = new HashSet<>();
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(tw.getOutDir(), "*Data.db"))
        {
            dataFileStream.forEach(dataFilePath -> {
                dataFilePaths.add(dataFilePath);
                assertEquals(CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version).getMajorVersion(),
                             SSTables.cassandraVersionFromTable(dataFilePath).getMajorVersion());
            });
        }
        // no exception should be thrown from both the validate methods
        tw.validateSSTables(writerContext);
        tw.validateSSTables(writerContext, dataFilePaths);
    }
}
