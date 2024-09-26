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

import java.io.FileInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BundleTest
{
    @TempDir
    private Path tempFolder;

    @Test
    void testBuildBundleAndDelete() throws Exception
    {
        Path stagingDir = Files.createDirectories(tempFolder.resolve("staging"));
        long totalSize = 0;
        int sstableCount = 3;
        int componentCount = 5;
        List<SSTableFilesAndRange> sourceSSTables = new ArrayList<>(sstableCount);
        for (int i = 0; i < sstableCount; i++)
        {
            sourceSSTables.add(mockSSTableFilesAndRange(componentCount, 100));
            totalSize += 100;
        }
        Bundle bundle = Bundle.builder()
                              .bundleSequence(0)
                              .sourceSSTables(sourceSSTables)
                              .bundleNameGenerator(new BundleNameGenerator("jobId", "sessionId"))
                              .bundleStagingDirectory(stagingDir)
                              .build();
        assertEquals(totalSize, bundle.bundleUncompressedSize);
        assertEquals(BigInteger.ONE, bundle.startToken);
        assertEquals(BigInteger.TEN, bundle.endToken);
        assertNotNull(bundle.bundleFile);
        assertTrue(Files.exists(bundle.bundleFile));
        ZipInputStream zis = new ZipInputStream(new FileInputStream(bundle.bundleFile.toFile()));
        int acutalFilesCount = 0;
        while (zis.getNextEntry() != null)
        {
            acutalFilesCount++;
        }
        // the extra file (+ 1) is the manifest file
        assertEquals(sstableCount * componentCount + 1, acutalFilesCount);

        bundle.deleteAll();
        assertFalse(Files.exists(bundle.bundleFile));
        assertFalse(Files.exists(bundle.bundleDirectory));
        long filesCount = Files.list(stagingDir).count();
        assertEquals(0, filesCount);
    }

    private SSTableFilesAndRange mockSSTableFilesAndRange(int fileCount, long size) throws Exception
    {
        SSTableSummary summary = new SSTableSummary(BigInteger.ONE, BigInteger.TEN,
                                                    UUID.randomUUID().toString());
        List<Path> paths = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++)
        {
            paths.add(Files.createFile(tempFolder.resolve(UUID.randomUUID().toString())));
        }
        return new SSTableFilesAndRange(summary, paths, size);
    }
}
