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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;
import org.apache.cassandra.spark.common.Digest;

import static org.assertj.core.api.Assertions.assertThat;

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
        Map<Path, Digest> fileDigests = new HashMap<>();
        for (SSTableFilesAndRange sstable : sourceSSTables)
        {
            for (Path file : sstable.files)
            {
                fileDigests.put(file, mockDigest(file.getFileName().toString()));
            }
        }
        Bundle bundle = Bundle.builder()
                              .bundleSequence(0)
                              .sourceSSTables(sourceSSTables)
                              .bundleNameGenerator(new BundleNameGenerator("jobId", "sessionId"))
                              .bundleStagingDirectory(stagingDir)
                              .fileDigests(fileDigests)
                              .build();
        assertThat(bundle.bundleUncompressedSize).isEqualTo(totalSize);
        assertThat(bundle.firstToken).isEqualTo(BigInteger.ONE);
        assertThat(bundle.endToken).isEqualTo(BigInteger.TEN);
        assertThat(bundle.bundleFile).isNotNull();
        assertThat(Files.exists(bundle.bundleFile)).isTrue();
        ZipInputStream zis = new ZipInputStream(new FileInputStream(bundle.bundleFile.toFile()));
        int acutalFilesCount = 0;
        ZipEntry entry = null;
        ObjectMapper objectMapper = new ObjectMapper();
        boolean hasManifest = false;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((entry = zis.getNextEntry()) != null)
        {
            acutalFilesCount++;
            if (entry.getName().endsWith("manifest.json"))
            {
                hasManifest = true;
                zis.transferTo(baos);
            }
        }
        assertThat(hasManifest).isTrue();
        Map manifest = objectMapper.readValue(baos.toByteArray(), Map.class);
        assertManifestEntries(manifest, sourceSSTables);
        // the extra file (+ 1) is the manifest file
        assertThat(acutalFilesCount).isEqualTo(sstableCount * componentCount + 1);

        bundle.deleteAll();
        assertThat(Files.exists(bundle.bundleFile)).isFalse();
        assertThat(Files.exists(bundle.bundleDirectory)).isFalse();
        long filesCount = Files.list(stagingDir).count();
        assertThat(filesCount).isEqualTo(0);
    }

    private void assertManifestEntries(Map manifest, List<SSTableFilesAndRange> sourceSSTables)
    {
        Map<String, String> digests = new HashMap<>();
        manifest.values().forEach(value -> {
            Map<String, String> componentsChecksum = (Map<String, String>) ((Map<String, Object>) value).get("components_checksum");
            digests.putAll(componentsChecksum);
        });

        for (SSTableFilesAndRange sstable : sourceSSTables)
        {
            for (Path file : sstable.files)
            {
                String fileName = file.getFileName().toString();
                assertThat(digests.getOrDefault(fileName, "File: " + fileName + " is not found in manifest"))
                        .as("The digest in the manifest.json does not match with the filename (test configures filename as digest)")
                        .isEqualTo(fileName);
            }
        }
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

    private Digest mockDigest(String checksum)
    {
        return new Digest()
        {
            @Override
            public String value()
            {
                return checksum;
            }

            @Override
            public o.a.c.sidecar.client.shaded.common.request.data.Digest toSidecarDigest()
            {
                return null;
            }
        };
    }
}
