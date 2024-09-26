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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.util.IOUtils;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.utils.TemporaryDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SSTablesBundlerTest
{
    private final String jobId = UUID.randomUUID().toString();
    private final String sessionId = "1-" + UUID.randomUUID();

    @Test
    void testNumberOfBundlesGenerated() throws IOException, URISyntaxException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, sessionId);

            Path sourceDir = Paths.get(getClass().getResource("/data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0").toURI());
            Path outputDir = tempDir.path();
            FileUtils.copyDirectory(sourceDir.toFile(), outputDir.toFile());

            CassandraBridge bridge = mockCassandraBridge(outputDir);
            SSTableLister ssTableLister = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            SSTablesBundler ssTablesBundler = new SSTablesBundler(outputDir, ssTableLister, nameGenerator, 5 * 1024);
            ssTablesBundler.includeDirectory(outputDir);
            ssTablesBundler.finish();
            List<Bundle> bundles = ImmutableList.copyOf(ssTablesBundler);
            assertEquals(2, bundles.size());

            Path bundle0 = outputDir.resolve("0");
            Path bundle1 = outputDir.resolve("1");
            assertTrue(Files.exists(bundle0) && Files.isDirectory(bundle0));
            assertTrue(Files.exists(bundle1) && Files.isDirectory(bundle1));
            String expectedZippedBundlePath1 = "b_" + jobId + "_" + sessionId + "_1_3";
            String expectedZippedBundlePath2 = "e_" + jobId + "_" + sessionId + "_4_6";
            assertTrue(Files.exists(outputDir.resolve(expectedZippedBundlePath1)));
            assertTrue(Files.exists(outputDir.resolve(expectedZippedBundlePath2)));
        }
    }

    @Test
    void testManifestWritten() throws IOException, URISyntaxException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, sessionId);

            Path sourceDir = Paths.get(getClass().getResource("/data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0").toURI());
            Path outputDir = tempDir.path();
            FileUtils.copyDirectory(sourceDir.toFile(), outputDir.toFile());

            CassandraBridge bridge = mockCassandraBridge(outputDir);
            SSTableLister writerOutputAnalyzer = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            SSTablesBundler ssTablesBundler = new SSTablesBundler(outputDir, writerOutputAnalyzer, nameGenerator, 5 * 1024);
            ssTablesBundler.includeDirectory(outputDir);
            ssTablesBundler.finish();
            // evaluate and compute all bundles
            while (ssTablesBundler.hasNext())
            {
                ssTablesBundler.next();
            }

            String expectedBundle0Manifest = "{\n" +
                                             "  \"na-1-big-\" : {\n" +
                                             "    \"components_checksum\" : {\n" +
                                             "      \"na-1-big-Summary.db\" : \"e2c32c23\",\n" +
                                             "      \"na-1-big-TOC.txt\" : \"7c8ef1f5\",\n" +
                                             "      \"na-1-big-Filter.db\" : \"72fc4f9c\",\n" +
                                             "      \"na-1-big-Index.db\" : \"ee128018\",\n" +
                                             "      \"na-1-big-Data.db\" : \"f48b39a3\",\n" +
                                             "      \"na-1-big-Statistics.db\" : \"f773fcc6\"\n" +
                                             "    },\n" +
                                             "    \"start_token\" : 1,\n" +
                                             "    \"end_token\" : 3\n" +
                                             "  }\n" +
                                             "}";
            String expectedBundle1Manifest = "{\n"
                                             + "  \"na-2-big-\" : {\n"
                                             + "    \"components_checksum\" : {\n"
                                             + "      \"na-2-big-Filter.db\" : \"72fc4f9c\",\n"
                                             + "      \"na-2-big-TOC.txt\" : \"7c8ef1f5\",\n"
                                             + "      \"na-2-big-Index.db\" : \"ee128018\",\n"
                                             + "      \"na-2-big-Data.db\" : \"f48b39a3\",\n"
                                             + "      \"na-2-big-Summary.db\" : \"e2c32c23\",\n"
                                             + "      \"na-2-big-Statistics.db\" : \"f773fcc6\"\n"
                                             + "    },\n"
                                             + "    \"start_token\" : 4,\n"
                                             + "    \"end_token\" : 6\n"
                                             + "  }\n"
                                             + "}";
            Path bundle0Manifest = outputDir.resolve("0").resolve("manifest.json");
            Path bundle1Manifest = outputDir.resolve("1").resolve("manifest.json");
            assertTrue(Files.exists(bundle0Manifest));
            assertTrue(Files.exists(bundle1Manifest));
            ObjectMapper mapper = new ObjectMapper();
            Map actualBundle0 = mapper.readValue(bundle0Manifest.toFile(), Map.class);
            Map expectedBundle0 = mapper.readValue(expectedBundle0Manifest, Map.class);
            assertEquals(expectedBundle0, actualBundle0);
            Map actualBundle1 = mapper.readValue(bundle1Manifest.toFile(), Map.class);
            Map expectedBundle1 = mapper.readValue(expectedBundle1Manifest, Map.class);
            assertEquals(expectedBundle1, actualBundle1);
        }
    }

    @Test
    void testChecksumComputedForEmptyFile() throws IOException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            Path empty = Files.createFile(tempDir.path().resolve("empty"));
            assertEquals("2cc5d05", IOUtils.xxhash32(empty));
        }
    }

    @Test
    void testEmptyOutputDir() throws IOException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, sessionId);

            Path outputDir = tempDir.path();
            CassandraBridge bridge = mockCassandraBridge(outputDir);
            SSTableLister ssTableLister = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            SSTablesBundler ssTablesBundler = new SSTablesBundler(outputDir, ssTableLister, nameGenerator, 200);
            assertThrows(NoSuchElementException.class, ssTablesBundler::next);
        }
    }

    private CassandraBridge mockCassandraBridge(Path outputDir)
    {
        CassandraBridge bridge = mock(CassandraBridge.class);

        SSTableSummary summary1 = new SSTableSummary(BigInteger.valueOf(1L), BigInteger.valueOf(3L), "na-1-big-");
        SSTableSummary summary2 = new SSTableSummary(BigInteger.valueOf(4L), BigInteger.valueOf(6L), "na-2-big-");

        FileSystemSSTable ssTable1 = new FileSystemSSTable(outputDir.resolve("na-1-big-Data.db"), false, null);
        FileSystemSSTable ssTable2 = new FileSystemSSTable(outputDir.resolve("na-2-big-Data.db"), false, null);
        when(bridge.getSSTableSummary("ks", "table1", ssTable1)).thenReturn(summary1);
        when(bridge.getSSTableSummary("ks", "table1", ssTable2)).thenReturn(summary2);
        return bridge;
    }
}
