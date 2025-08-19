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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.DigestAlgorithms;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.spark.utils.TemporaryDirectory;

import static org.assertj.core.api.Assertions.assertThat;
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
        sstableLister.includeFileDigests(calculateFileDigests(outputDir));
        List<SSTableLister.SSTableFilesAndRange> sstables = new ArrayList<>();
        // 10196 is the total size of files in /data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0
        // If this line fails, maybe something has been changed in the folder.
        assertThat(sstableLister.totalSize()).isEqualTo(10196);
        while (!sstableLister.isEmpty())
        {
            sstables.add(sstableLister.consumeOne());
        }
        assertThat(sstables).hasSize(2);
        Set<String> ssTablePrefixes = sstables.stream()
                                              .map(sstable -> sstable.summary.sstableId)
                                              .collect(Collectors.toSet());

        assertThat(ssTablePrefixes).contains("na-1-big-");
        assertThat(ssTablePrefixes).contains("na-2-big-");

        Set<Path> range1Files = sstables.get(0).files;
        Set<Path> range2Files = sstables.get(1).files;

        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Data.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Index.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Summary.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Statistics.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-TOC.txt"));

        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Data.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Index.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Summary.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Statistics.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-TOC.txt"));

        for (SSTableLister.SSTableFilesAndRange sstable : sstables)
        {
            for (Path file : sstable.files)
            {
                assertThat(sstableLister.fileDigests(Collections.singleton(file))).as("Digest for file should exist. file: " + file).isNotNull();
            }
        }
    }

    @Test
    void testEmptyDir() throws IOException
    {
        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            CassandraBridge bridge = mock(CassandraBridge.class);
            SSTableLister ssTableLister = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            ssTableLister.includeDirectory(tempDir.path());
            assertThat(ssTableLister.peek()).isNull();
            assertThat(ssTableLister.consumeOne()).isNull();
            assertThat(ssTableLister.isEmpty()).isTrue();
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
        assertThat(sstableLister.isEmpty()).isFalse();
        assertThat(sstableLister.totalSize()).isEqualTo(5098);
        while (!sstableLister.isEmpty())
        {
            sstables.add(sstableLister.consumeOne());
        }
        assertThat(sstableLister.totalSize()).isEqualTo(0);
        assertThat(sstables).hasSize(1);
        Set<Path> range1Files = sstables.get(0).files;
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Data.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Index.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Summary.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-Statistics.db"));
        assertThat(range1Files).contains(outputDir.resolve("na-1-big-TOC.txt"));

        // now include the entire directory
        // note that one sstable has been included. The sstable should be ignored when including the directory
        sstableLister.includeDirectory(outputDir);
        assertThat(sstableLister.isEmpty()).isFalse();
        assertThat(sstableLister.totalSize()).isEqualTo(5098);
        int producedSSTables = 0;
        while (!sstableLister.isEmpty())
        {
            producedSSTables += 1;
            sstables.add(sstableLister.consumeOne());
        }
        assertThat(producedSSTables).isEqualTo(1);
        assertThat(sstableLister.totalSize()).isEqualTo(0);
        assertThat(sstables).hasSize(2);

        Set<Path> range2Files = sstables.get(1).files;
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Data.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Index.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Summary.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-Statistics.db"));
        assertThat(range2Files).contains(outputDir.resolve("na-2-big-TOC.txt"));
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


    static Map<Path, Digest> calculateFileDigests(Path dir)
    {
        DigestAlgorithm digester = DigestAlgorithms.XXHASH32.get();
        Map<Path, Digest> result = new HashMap<>();
        try (Stream<Path> files = Files.walk(dir))
        {
            Iterator<Path> it = files.iterator();
            while (it.hasNext())
            {
                Path file = it.next();
                if (Files.isRegularFile(file))
                {
                    result.put(file, digester.calculateFileDigest(file));
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }
}
