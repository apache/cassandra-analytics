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

package org.apache.cassandra.spark.common;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.SSTableDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class SSTablesTest
{
    @Test
    public void testGetSSTableBaseNameBigFormat()
    {
        // Test Cassandra 4.0 big format SSTable
        Path dataFile = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        String baseName = SSTables.getSSTableBaseName(dataFile);
        assertThat(baseName).isEqualTo("oa-1-big");
    }

    @Test
    public void testGetSSTableBaseNameBtiFormat()
    {
        // Test Cassandra 5.0 bti format SSTable
        Path dataFile = Paths.get("/path/to/sstable/da-2-bti-Data.db");
        String baseName = SSTables.getSSTableBaseName(dataFile);
        assertThat(baseName).isEqualTo("da-2-bti");
    }

    @Test
    public void testGetSSTableBaseNameWithHigherGeneration()
    {
        // Test SSTable with higher generation number
        Path dataFile = Paths.get("/path/to/sstable/oa-12345-big-Data.db");
        String baseName = SSTables.getSSTableBaseName(dataFile);
        assertThat(baseName).isEqualTo("oa-12345-big");
    }

    @Test
    public void testGetSSTableBaseNameIndexFile()
    {
        // Test with Index.db file
        Path indexFile = Paths.get("/path/to/sstable/oa-1-big-Index.db");
        String baseName = SSTables.getSSTableBaseName(indexFile);
        assertThat(baseName).isEqualTo("oa-1-big");
    }

    @Test
    public void testGetSSTableBaseNameSummaryFile()
    {
        // Test with Summary.db file
        Path summaryFile = Paths.get("/path/to/sstable/oa-1-big-Summary.db");
        String baseName = SSTables.getSSTableBaseName(summaryFile);
        assertThat(baseName).isEqualTo("oa-1-big");
    }

    @Test
    public void testGetSSTableBaseNameFilterFile()
    {
        // Test with Filter.db file
        Path filterFile = Paths.get("/path/to/sstable/oa-1-big-Filter.db");
        String baseName = SSTables.getSSTableBaseName(filterFile);
        assertThat(baseName).isEqualTo("oa-1-big");
    }

    @Test
    public void testGetSSTableDescriptorBigFormat()
    {
        // Test SSTableDescriptor creation for big format
        Path dataFile = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        SSTableDescriptor descriptor = SSTables.getSSTableDescriptor(dataFile);
        assertThat(descriptor.baseFilename).isEqualTo("oa-1-big");
    }

    @Test
    public void testGetSSTableDescriptorBtiFormat()
    {
        // Test SSTableDescriptor creation for bti format
        Path dataFile = Paths.get("/path/to/sstable/da-2-bti-Data.db");
        SSTableDescriptor descriptor = SSTables.getSSTableDescriptor(dataFile);
        assertThat(descriptor.baseFilename).isEqualTo("da-2-bti");
    }

    @Test
    public void testSSTableDescriptorEquality()
    {
        // Test that descriptors created from different component files of the same SSTable are equal
        Path dataFile = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        Path indexFile = Paths.get("/path/to/sstable/oa-1-big-Index.db");
        Path summaryFile = Paths.get("/path/to/sstable/oa-1-big-Summary.db");

        SSTableDescriptor descriptor1 = SSTables.getSSTableDescriptor(dataFile);
        SSTableDescriptor descriptor2 = SSTables.getSSTableDescriptor(indexFile);
        SSTableDescriptor descriptor3 = SSTables.getSSTableDescriptor(summaryFile);

        assertThat(descriptor1).isEqualTo(descriptor2);
        assertThat(descriptor2).isEqualTo(descriptor3);
        assertThat(descriptor1).isEqualTo(descriptor3);
    }

    @Test
    public void testSSTableDescriptorInequalityDifferentGeneration()
    {
        // Test that descriptors for different SSTables are not equal
        Path dataFile1 = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        Path dataFile2 = Paths.get("/path/to/sstable/oa-2-big-Data.db");

        SSTableDescriptor descriptor1 = SSTables.getSSTableDescriptor(dataFile1);
        SSTableDescriptor descriptor2 = SSTables.getSSTableDescriptor(dataFile2);

        assertThat(descriptor1).isNotEqualTo(descriptor2);
    }

    @Test
    public void testSSTableDescriptorInequalityDifferentFormat()
    {
        // Test that descriptors for different formats are not equal
        Path bigFormatFile = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        Path btiFormatFile = Paths.get("/path/to/sstable/da-1-bti-Data.db");

        SSTableDescriptor descriptor1 = SSTables.getSSTableDescriptor(bigFormatFile);
        SSTableDescriptor descriptor2 = SSTables.getSSTableDescriptor(btiFormatFile);

        assertThat(descriptor1).isNotEqualTo(descriptor2);
    }

    @Test
    public void testDescriptorMatchesProducedSSTable()
    {
        // This test verifies the fix for the bug where descriptors created from file paths
        // didn't match descriptors created during SSTable production.
        //
        // When SSTableWriterImplementation.onSSTablesProduced creates a descriptor, it uses
        // CassandraBridgeImplementation.baseFilename which returns "oa-1-big" (no trailing dash).
        //
        // When SortedSSTableWriter.prepareSStablesToSend creates a descriptor from a file path,
        // it should also return "oa-1-big" (no trailing dash) so they match.

        Path dataFile = Paths.get("/path/to/sstable/oa-1-big-Data.db");
        SSTableDescriptor descriptorFromPath = SSTables.getSSTableDescriptor(dataFile);

        // Simulate what SSTableWriterImplementation.onSSTablesProduced does
        SSTableDescriptor descriptorFromProduction = new SSTableDescriptor("oa-1-big");

        // These must be equal for the filter in SortedSSTableWriter.prepareSStablesToSend to work
        assertThat(descriptorFromPath).isEqualTo(descriptorFromProduction);
        assertThat(descriptorFromPath.baseFilename).isEqualTo(descriptorFromProduction.baseFilename)
                                                   .doesNotEndWith("-");
    }
}
