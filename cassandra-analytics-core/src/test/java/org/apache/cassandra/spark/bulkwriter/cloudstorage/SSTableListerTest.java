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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.utils.TemporaryDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SSTableListerTest
{
    private Path outputDir;

    @Test
    void testOutput() throws URISyntaxException
    {
        SSTableLister sstableLister = setupSSTableLister();
        sstableLister.includeDirectory(outputDir);
        List<SSTableLister.SSTableFilesAndRange> sstables = new ArrayList<>();
        // 10196 is the total size of files in /data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0
        // If this line fails, maybe something has been changed in the folder.
        assertEquals(10196, sstableLister.totalSize());
        while (!sstableLister.isEmpty())
        {
            sstables.add(sstableLister.consumeOne());
        }
        assertEquals(2, sstables.size());
        Set<String> ssTablePrefixes = sstables.stream()
                                              .map(sstable -> sstable.summary.sstableId)
                                              .collect(Collectors.toSet());

        assertTrue(ssTablePrefixes.contains("na-1-big-"));
        assertTrue(ssTablePrefixes.contains("na-2-big-"));

        Set<Path> range1Files = sstables.get(0).files;
        Set<Path> range2Files = sstables.get(1).files;

        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Data.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Index.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Summary.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Statistics.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-TOC.txt")));

        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Data.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Index.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Summary.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Statistics.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-TOC.txt")));
    }

    @Test
    void testEmptyDir() throws IOException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            CassandraBridge bridge = mock(CassandraBridge.class);
            SSTableLister ssTableLister = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            ssTableLister.includeDirectory(tempDir.path());
            assertNull(ssTableLister.peek());
            assertNull(ssTableLister.consumeOne());
            assertTrue(ssTableLister.isEmpty());
        }
    }

    @Test
    void testIncludeSSTable() throws Exception
    {
        SSTableLister sstableLister = setupSSTableLister();
        List<Path> sstableComponents = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputDir, "na-1-big-*"))
        {
            stream.forEach(sstableComponents::add);
        }
        sstableLister.includeSSTable(sstableComponents);
        List<SSTableLister.SSTableFilesAndRange> sstables = new ArrayList<>();
        assertFalse(sstableLister.isEmpty());
        assertEquals(5098, sstableLister.totalSize());
        while (!sstableLister.isEmpty())
        {
            sstables.add(sstableLister.consumeOne());
        }
        assertEquals(0, sstableLister.totalSize());
        assertEquals(1, sstables.size());
        Set<Path> range1Files = sstables.get(0).files;
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Data.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Index.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Summary.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-Statistics.db")));
        assertTrue(range1Files.contains(outputDir.resolve("na-1-big-TOC.txt")));

        // now include the entire directory
        // note that one sstable has been included. The sstable should be ignored when including the directory
        sstableLister.includeDirectory(outputDir);
        assertFalse(sstableLister.isEmpty());
        assertEquals(5098, sstableLister.totalSize());
        int producedSSTables = 0;
        while (!sstableLister.isEmpty())
        {
            producedSSTables += 1;
            sstables.add(sstableLister.consumeOne());
        }
        assertEquals(1, producedSSTables);
        assertEquals(0, sstableLister.totalSize());
        assertEquals(2, sstables.size());

        Set<Path> range2Files = sstables.get(1).files;
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Data.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Index.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Summary.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-Statistics.db")));
        assertTrue(range2Files.contains(outputDir.resolve("na-2-big-TOC.txt")));
    }

    private SSTableLister setupSSTableLister() throws URISyntaxException
    {
        outputDir = Paths.get(getClass().getResource("/data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0").toURI());
        CassandraBridge bridge = mock(CassandraBridge.class);

        SSTableSummary summary1 = new SSTableSummary(BigInteger.valueOf(1L), BigInteger.valueOf(3L), "na-1-big-");
        SSTableSummary summary2 = new SSTableSummary(BigInteger.valueOf(3L), BigInteger.valueOf(6L), "na-2-big-");

        FileSystemSSTable ssTable1 = new FileSystemSSTable(outputDir.resolve("na-1-big-Data.db"), false, null);
        FileSystemSSTable ssTable2 = new FileSystemSSTable(outputDir.resolve("na-2-big-Data.db"), false, null);
        when(bridge.getSSTableSummary("ks", "table1", ssTable1)).thenReturn(summary1);
        when(bridge.getSSTableSummary("ks", "table1", ssTable2)).thenReturn(summary2);
        return new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
    }
}
