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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.Stats;

/**
 * {@link SSTableLister} lists the directories containing SSTables.
 * Internally, the listed SSTables are sorted by the insertion order of the directories,
 * and by the first and end token of the SSTables.
 * Therefore, it is expected that the SSTables are sorted when consuming from the lister.
 */
public class SSTableLister implements SSTableCollector
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableLister.class);
    private static final Comparator<SSTableFilesAndRange> SORT_BY_FIRST_TOKEN_THEN_LAST_TOKEN =
    Comparator.<SSTableFilesAndRange, BigInteger>comparing(sstable -> sstable.summary.firstToken)
              .thenComparing(sstable -> sstable.summary.lastToken);
    private final QualifiedTableName qualifiedTableName;
    private final CassandraBridge bridge;
    private final Queue<SSTableFilesAndRange> sstables;
    private final Set<Path> sstableDirectories;
    private long totalSize;

    public SSTableLister(QualifiedTableName qualifiedTableName, CassandraBridge bridge)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.bridge = bridge;
        this.sstables = new LinkedBlockingQueue<>();
        this.sstableDirectories = new HashSet<>();
    }

    @Override
    public void includeDirectory(Path dir)
    {
        if (!sstableDirectories.add(dir))
        {
            throw new IllegalArgumentException("The directory has been included already! Input dir: " + dir
                                               + "; existing directories: " + sstableDirectories);
        }

        listSSTables(dir)
        .map(components -> {
            SSTable sstable = buildSSTable(components);
            SSTableSummary summary = bridge.getSSTableSummary(qualifiedTableName.keyspace(),
                                                              qualifiedTableName.table(),
                                                              sstable);
            long size = sizeSum(components);
            totalSize += size;
            return new SSTableFilesAndRange(summary, components, sizeSum(components));
        })
        .sorted(SORT_BY_FIRST_TOKEN_THEN_LAST_TOKEN)
        .forEach(sstables::add);
    }

    @Override
    public void includeSSTable(List<Path> sstableComponents)
    {
        // TODO: refactor
        SSTable sstable = buildSSTable(sstableComponents);
        SSTableSummary summary = bridge.getSSTableSummary(qualifiedTableName.keyspace(),
                                                          qualifiedTableName.table(),
                                                          sstable);
        long size = sizeSum(sstableComponents);
        totalSize += size;
        SSTableFilesAndRange sstableAndRange = new SSTableFilesAndRange(summary, sstableComponents, sizeSum(sstableComponents));
        sstables.add(sstableAndRange);
    }

    @Override
    public long totalSize()
    {
        return totalSize;
    }

    @Override
    public SSTableFilesAndRange peek()
    {
        return sstables.peek();
    }

    @Override
    public SSTableFilesAndRange consumeOne()
    {
        SSTableFilesAndRange sstable = sstables.poll();
        if (sstable != null)
        {
            totalSize -= sstable.size;
        }

        return sstable;
    }

    @Override
    public boolean isEmpty()
    {
        return sstables.isEmpty();
    }

    private Stream<List<Path>> listSSTables(Path dir)
    {
        Map<String, List<Path>> componentsByPrefix = new HashMap<>();
        try (Stream<Path> stream = Files.list(dir))
        {
            stream.forEach(path -> {
                final String ssTablePrefix = getSSTablePrefix(path.getFileName().toString());

                if (ssTablePrefix.isEmpty())
                {
                    // ignore files that are not SSTables components
                    return;
                }

                List<Path> prefixPaths = componentsByPrefix.computeIfAbsent(ssTablePrefix, ignored -> new ArrayList<>(8));
                prefixPaths.add(path);
            });
            return componentsByPrefix.values().stream();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private long sizeSum(List<Path> files)
    {
        return files
               .stream()
               .mapToLong(path -> {
                   try
                   {
                       BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class,
                                                                                 // not expecting links and do not follow links
                                                                                 LinkOption.NOFOLLOW_LINKS);
                       if (fileAttributes != null && fileAttributes.isRegularFile())
                       {
                           return fileAttributes.size();
                       }
                       else
                       {
                           return 0L;
                       }
                   }
                   catch (IOException e)
                   {
                       LOGGER.warn("Failed to get size of file. path={}", path);
                       return 0L;
                   }
               })
               .sum();
    }

    private String getSSTablePrefix(String componentName)
    {
        return componentName.substring(0, componentName.lastIndexOf('-') + 1);
    }

    private SSTable buildSSTable(List<Path> components)
    {
        List<Path> dataComponents = components.stream()
                                              .filter(path -> path.getFileName().toString().contains("Data.db"))
                                              .collect(Collectors.toList());
        if (dataComponents.size() != 1)
        {
            throw new IllegalArgumentException("SSTable should have only one data component");
        }
        return new FileSystemSSTable(dataComponents.get(0), true, Stats.DoNothingStats.INSTANCE::bufferingInputStreamStats);
    }
}
