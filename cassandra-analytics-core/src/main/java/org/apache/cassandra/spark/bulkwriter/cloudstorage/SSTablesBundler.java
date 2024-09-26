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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;

/**
 * {@link SSTablesBundler} bundles SSTables in the output directory provided by
 * {@link org.apache.cassandra.bridge.SSTableWriter}. With output from {@link SSTableLister}, we get sorted
 * list of {@link SSTableFilesAndRange}. According to sorted order, we move all component files
 * related to a SSTable into bundle folder. When a bundle's size exceeds configured, a new bundle is created and
 * SSTable components are moved into new bundle folder.
 * <br>
 * When a bundle is being closed, {@link Bundle} generated for that bundle gets written to manifest.json file
 * and added to bundle folder. The entire folder is then zipped and added to zipped_bundles folder
 * <br>
 * Under output directory of {@link org.apache.cassandra.bridge.SSTableWriter}, sample folders created look like
 * bundle0, bundle1, bundle2, zipped_bundles
 */
public class SSTablesBundler implements Iterator<Bundle>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTablesBundler.class);
    private final SSTableCollector collector;
    private final BundleNameGenerator bundleNameGenerator;
    private final Path bundleStagingDir;
    private final long maxSizePerBundleInBytes;
    private boolean reachedEnd = false;
    private int bundleIndex = 0;
    private Bundle currentBundle = null;

    public SSTablesBundler(Path bundleStagingDir, SSTableCollector collector,
                           BundleNameGenerator bundleNameGenerator, long maxSizePerBundleInBytes)
    {
        this.bundleStagingDir = bundleStagingDir;
        this.collector = collector;
        this.bundleNameGenerator = bundleNameGenerator;
        this.maxSizePerBundleInBytes = maxSizePerBundleInBytes;
    }

    @Override
    public boolean hasNext()
    {
        if (reachedEnd)
        {
            // consume all sstables from collector
            return !collector.isEmpty();
        }
        else
        {
            // consume only when sstables have enough total size
            return collector.totalSize() > maxSizePerBundleInBytes;
        }
    }

    @Override
    public Bundle next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException("Bundles have exhausted");
        }

        try
        {
            currentBundle = computeNext();
            return currentBundle;
        }
        catch (Exception exception)
        {
            throw new RuntimeException("Unable to produce bundle", exception);
        }
    }

    public void includeDirectory(Path dir)
    {
        collector.includeDirectory(dir);
    }

    public void includeSSTable(List<Path> sstableComponents)
    {
        collector.includeSSTable(sstableComponents);
    }

    public void finish()
    {
        reachedEnd = true;
    }

    public void cleanupBundle(String sessionID)
    {
        LOGGER.info("[{}]: Clean up bundle files after stream session bundle={}", sessionID, currentBundle);
        if (currentBundle == null)
        {
            return;
        }

        try
        {
            Bundle bundle = currentBundle;
            currentBundle = null;
            bundle.deleteAll();
        }
        catch (IOException exception)
        {
            LOGGER.warn("[{}]: Failed to clean up bundle files bundle={}", sessionID, currentBundle, exception);
        }
    }

    private Bundle computeNext() throws IOException
    {
        List<SSTableFilesAndRange> sstableFiles = new ArrayList<>();
        long size = 0;
        while (!collector.isEmpty())
        {
            SSTableFilesAndRange sstable = collector.peek();
            long lastSize = size;
            size += sstable.size;
            // Stop adding more, _only_ when
            // 1) it has included some sstables already, and
            // 2) adding this one will exceed the size limit
            // It means that if the first sstable included in the loop is larger than the limit,
            // the large sstable is added regardless.
            if (size > maxSizePerBundleInBytes && lastSize != 0)
            {
                break;
            }
            else
            {
                sstableFiles.add(sstable);
                collector.consumeOne();
            }
        }

        // if not exist yet, create folder for holding all zipped bundles
        Files.createDirectories(bundleStagingDir);
        return Bundle.builder()
                     .bundleSequence(bundleIndex++)
                     .bundleStagingDirectory(bundleStagingDir)
                     .sourceSSTables(sstableFiles)
                     .bundleNameGenerator(bundleNameGenerator)
                     .build();
    }

    static long zip(Path sourcePath, Path targetPath) throws IOException
    {
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(targetPath));
             Stream<Path> stream = Files.walk(sourcePath, 1))
        {
            stream.filter(Files::isRegularFile)
                  .forEach(path -> {
                      ZipEntry zipEntry = new ZipEntry(sourcePath.relativize(path).toString());
                      try
                      {
                          zos.putNextEntry(zipEntry);
                          Files.copy(path, zos);
                          zos.closeEntry();
                      }
                      catch (IOException e)
                      {
                          LOGGER.error("Unexpected error while zipping file. path={}", path, e);
                          throw new RuntimeException(e);
                      }
                  });
        }
        return targetPath.toFile().length();
    }
}
