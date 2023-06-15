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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.jetbrains.annotations.NotNull;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SSTableWriterTest
{
    private static String previousMbeanState;

    @Parameterized.Parameters(name = "{index}: Testing Cassandra Version {0}")
    public static Iterable<Object[]> data()
    {
        return Arrays.stream(CassandraVersion.supportedVersions())
                     .map(version -> new Object[]{version})
                     .collect(Collectors.toList());
    }

    @BeforeClass
    public static void setProps()
    {
        previousMbeanState = System.getProperty("org.apache.cassandra.disable_mbean_registration");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    @AfterClass
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

    @NotNull
    public CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test", 12);  // CHECKSTYLE IGNORE: Public mutable field

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Parameterized.Parameter(0)
    public String version;  // CHECKSTYLE IGNORE: Public mutable field for parameterized testing

    @Test
    public void canCreateWriterForVersion() throws IOException
    {
        MockBulkWriterContext writerContext = new MockBulkWriterContext(ring, version, ConsistencyLevel.CL.LOCAL_QUORUM);
        SSTableWriter tw = new SSTableWriter(writerContext, tmpDir.getRoot().toPath());
        tw.addRow(BigInteger.ONE, ImmutableMap.of("id", 1, "date", 1, "course", "foo", "marks", 1));
        tw.close(writerContext, 1);
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(tw.getOutDir(), "*Data.db"))
        {
            dataFileStream.forEach(dataFilePath ->
                    assertEquals(CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(version).getMajorVersion(),
                                 SSTables.cassandraVersionFromTable(dataFilePath).getMajorVersion()));
        }
        tw.validateSSTables(writerContext, 1);
    }
}
