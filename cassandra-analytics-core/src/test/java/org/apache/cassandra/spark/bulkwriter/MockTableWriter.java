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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;

import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.bridge.SSTableWriter;
import org.apache.cassandra.spark.utils.DigestAlgorithm;

public class MockTableWriter implements SSTableWriter
{
    private static final String BASE_NAME = "/test-test-me-1-big-";
    private static final String[] TABLE_COMPONENTS = {"Data.db",
                                                      "Index.db",
                                                      "Filter.db",
                                                      "CompressionInfo.db",
                                                      "Statistics.db",
                                                      "Digest.crc32",
                                                      "Summary.db",
                                                      "TOC.txt"};

    private Path outDir;
    private boolean addRowThrows;
    private final ArrayList<Object[]> rows = new ArrayList<>();
    private boolean closed = false;

    public MockTableWriter(Path outDir)
    {
        this.outDir = outDir;
    }

    public MockTableWriter setOutDir(Path outDir)
    {
        this.outDir = outDir;
        return this;
    }

    public void setAddRowThrows(boolean addRowThrows)
    {
        this.addRowThrows = addRowThrows;
    }

    @Override
    public void addRow(Map<String, Object> values) throws IOException
    {
        if (addRowThrows)
        {
            throw new RuntimeException("Failed to write because addRow throws");
        }
        rows.add(values.values().toArray());
    }

    @Override
    public void setSSTablesProducedListener(Consumer<Set<SSTableDescriptor>> listener)
    {
        // do nothing
    }

    @Override
    public void close() throws IOException
    {
        // Create files to mimic SSTableWriter
        for (String component: TABLE_COMPONENTS)
        {
            Path path = Paths.get(outDir.toString(), BASE_NAME + component);
            System.out.format("Writing mock component %s\n", path);
            Files.createFile(path);
        }
        closed = true;
    }

    @VisibleForTesting
    public Path getOutDir()
    {
        return outDir;
    }

    @VisibleForTesting
    public void removeOutDir() throws IOException
    {
        FileUtils.deleteDirectory(outDir.toFile());
    }

    public interface Creator
    {
        // to match with SortedSSTableWriter's constructor
        SortedSSTableWriter create(MockTableWriter tableWriter,
                                   Path outDir,
                                   DigestAlgorithm digestAlgorithm,
                                   int partitionId);
    }
}
