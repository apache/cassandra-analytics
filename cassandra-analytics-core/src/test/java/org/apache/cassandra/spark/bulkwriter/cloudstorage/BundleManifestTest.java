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

import java.math.BigInteger;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.core.JsonProcessingException;

import static org.apache.cassandra.spark.bulkwriter.cloudstorage.BundleManifest.Entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BundleManifestTest
{
    @TempDir
    private Path tempFolder;

    @Test
    void testJsonSerialization() throws JsonProcessingException
    {
        BundleManifest bundleManifest = testManifest();
        String value = BundleManifest.OBJECT_WRITER.writeValueAsString(bundleManifest);
        assertEquals(EXPECTED_JSON, value);
    }

    @Test
    void testPersistToFile() throws Exception
    {
        Path manifestFile = tempFolder.resolve("manifest.json");
        assertFalse(Files.exists(manifestFile));
        BundleManifest bundleManifest = testManifest();
        bundleManifest.persistTo(manifestFile);
        String persistedContent = FileUtils.readFileToString(manifestFile.toFile());
        assertEquals(EXPECTED_JSON, persistedContent);
    }

    @Test
    void testPersistToFileFailsWithExistingFile() throws Exception
    {
        // the file already exist
        Path manifestFile = Files.createFile(tempFolder.resolve("manifest.json"));
        assertTrue(Files.exists(manifestFile));
        assertThrows(FileAlreadyExistsException.class,
                     () -> testManifest().persistTo(manifestFile));
    }

    private BundleManifest testManifest()
    {
        BundleManifest bundleManifest = new BundleManifest();
        Entry manifestEntry1 = new Entry("prefix1",
                                         BigInteger.valueOf(1L),
                                         BigInteger.valueOf(3L));
        manifestEntry1.addComponentChecksum("prefix1-data.db", "checksumVal");
        manifestEntry1.addComponentChecksum("prefix1-statistics.db", "checksumVal");

        Entry manifestEntry2 = new Entry("prefix2",
                                         BigInteger.valueOf(4L),
                                         BigInteger.valueOf(6L));
        manifestEntry2.addComponentChecksum("prefix2-data.db", "checksumVal");
        manifestEntry2.addComponentChecksum("prefix2-statistics.db", "checksumVal");

        bundleManifest.addEntry(manifestEntry1);
        bundleManifest.addEntry(manifestEntry2);

        return bundleManifest;
    }

    private static final String EXPECTED_JSON = "{\n"
                                                + "  \"prefix2\" : {\n"
                                                + "    \"components_checksum\" : {\n"
                                                + "      \"prefix2-data.db\" : \"checksumVal\",\n"
                                                + "      \"prefix2-statistics.db\" : \"checksumVal\"\n"
                                                + "    },\n"
                                                + "    \"start_token\" : 4,\n"
                                                + "    \"end_token\" : 6\n"
                                                + "  },\n"
                                                + "  \"prefix1\" : {\n"
                                                + "    \"components_checksum\" : {\n"
                                                + "      \"prefix1-data.db\" : \"checksumVal\",\n"
                                                + "      \"prefix1-statistics.db\" : \"checksumVal\"\n"
                                                + "    },\n"
                                                + "    \"start_token\" : 1,\n"
                                                + "    \"end_token\" : 3\n"
                                                + "  }\n"
                                                + "}";
}
