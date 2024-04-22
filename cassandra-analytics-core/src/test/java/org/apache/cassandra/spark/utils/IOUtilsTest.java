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

package org.apache.cassandra.spark.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.util.IOUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IOUtilsTest
{
    @TempDir
    private Path tempFolder;

    @Test
    void testZip() throws Exception
    {
        File zipSourceDir = Files.createDirectories(tempFolder.resolve("zipSource")).toFile();
        int expectedFileCount = 10;
        for (int i = 0; i < expectedFileCount; i++)
        {
            new File(zipSourceDir, Integer.toString(i)).createNewFile();
        }
        File targetZip = tempFolder.resolve("zip").toFile();
        long zipFileSize = IOUtils.zip(zipSourceDir.toPath(), targetZip.toPath());
        assertTrue(targetZip.exists());
        assertTrue(zipFileSize > 0);

        ZipInputStream zis = new ZipInputStream(new FileInputStream(targetZip));
        int acutalFilesCount = 0;
        while (zis.getNextEntry() != null)
        {
            acutalFilesCount++;
        }
        assertEquals(expectedFileCount, acutalFilesCount);
    }

    @Test
    void testZipFailsOnInvalidInput()
    {
        Path file = tempFolder.resolve("file");
        IOException thrown = assertThrows(IOException.class,
                                          () -> IOUtils.zip(file, file));
        assertTrue(thrown.getMessage().contains("Not a directory"));
    }

    @Test
    void testChecksumCalculationShouldBeDeterministic() throws Exception
    {
        Path file = tempFolder.resolve("file");
        Files.write(file, "Hello World!".getBytes(StandardCharsets.UTF_8));
        String checksum1 = IOUtils.xxhash32(file);
        String checksum2 = IOUtils.xxhash32(file);
        assertEquals(checksum1, checksum2,
                     "Deterministic checksum calculation should yield same result for same input");
        assertEquals("bd69788", checksum1);

        Path anotherFile = tempFolder.resolve("anotherFile");
        Files.write(anotherFile, "Hello World!".getBytes(StandardCharsets.UTF_8));
        String checksum3 = IOUtils.xxhash32(anotherFile);
        assertEquals(checksum1, checksum3, "Checksum should be same for the same content");
    }

    @Test
    void testChecksumShouldBeDifferentForDifferentContent() throws Exception
    {
        Path file1 = tempFolder.resolve("file1");
        Path file2 = tempFolder.resolve("file2");
        Files.write(file1, "I am in file 1".getBytes(StandardCharsets.UTF_8));
        Files.write(file2, "File 2 is where you find me".getBytes(StandardCharsets.UTF_8));
        String checksum1 = IOUtils.xxhash32(file1);
        String checksum2 = IOUtils.xxhash32(file2);
        assertNotEquals(checksum1, checksum2);
        assertEquals("a6a6a5ba", checksum1);
        assertEquals("9e9b9db5", checksum2);
    }
}
