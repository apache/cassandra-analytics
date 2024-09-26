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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;
import org.apache.cassandra.spark.bulkwriter.util.IOUtils;
import org.apache.cassandra.spark.common.DataObjectBuilder;

/**
 * Bundle represents a set of SSTables bundled, as per bundle size set by clients through writer option.
 * {@link SSTablesBundler} can create multiple bundles, {@link #bundleSequence} is used to order the produced bundles.
 */
public class Bundle
{
    private static final String MANIFEST_FILE_NAME = "manifest.json";

    public final BigInteger startToken;
    public final BigInteger endToken;
    public final long bundleUncompressedSize;
    public final long bundleCompressedSize;
    // path to the bundle directory, which contains multiple files
    public final Path bundleDirectory;
    // path to the bundle zip file (single file)
    public final Path bundleFile;
    public final int bundleSequence;

    // private access for internal and mutable states
    private final BundleManifest bundleManifest;
    private final List<SSTableCollector.SSTableFilesAndRange> sourceSSTables;

    static Builder builder()
    {
        return new Builder();
    }

    protected Bundle(Builder builder)
    {
        this.startToken = builder.startToken;
        this.endToken = builder.endToken;
        this.bundleManifest = builder.bundleManifest;
        this.bundleUncompressedSize = builder.bundleUncompressedSize;
        this.bundleCompressedSize = builder.bundleCompressedSize;
        this.bundleDirectory = builder.bundleDirectory;
        this.bundleFile = builder.bundleFile;
        this.bundleSequence = builder.bundleSequence;
        this.sourceSSTables = builder.sourceSSTables;
    }

    public void deleteAll() throws IOException
    {
        List<IOException> ioExceptions = new ArrayList<>();
        sourceSSTables.forEach(sstable -> sstable.files.forEach(path -> {
            try
            {
                Files.deleteIfExists(path);
            }
            catch (IOException e)
            {
                ioExceptions.add(e);
            }
        }));

        try
        {
            FileUtils.deleteDirectory(bundleDirectory.toFile());
        }
        catch (IOException e)
        {
            ioExceptions.add(e);
        }

        try
        {
            Files.deleteIfExists(bundleFile);
        }
        catch (IOException e)
        {
            ioExceptions.add(e);
        }

        if (!ioExceptions.isEmpty())
        {
            IOException ioe = new IOException("Failed to delete all files of a bundle");
            ioExceptions.forEach(ioe::addSuppressed);
            throw ioe;
        }
    }

    @VisibleForTesting
    BundleManifest.Entry manifestEntry(String key)
    {
        return bundleManifest.get(key);
    }

    @Override
    public String toString()
    {
        return "BundleManifest{entryCount: " + bundleManifest.size()
               + ", bundleSequence: " + bundleSequence
               + ", bundleFile: " + bundleFile
               + ", uncompressedSize: " + bundleUncompressedSize
               + ", compressedSize: " + bundleCompressedSize
               + ", startToken: " + startToken
               + ", endToken: " + endToken + '}';
    }

    /**
     * Builder for {@link Bundle}
     */
    static class Builder implements DataObjectBuilder<Builder, Bundle>
    {
        private final BundleManifest bundleManifest;

        private BigInteger startToken;
        private BigInteger endToken;
        private Path bundleStagingDirectory;
        private Path bundleDirectory; // path of the directory that include sstables and manifest file to be bundled
        private Path bundleFile; // path of the bundle/zip file, which is uploaded to s3
        private int bundleSequence;
        private List<SSTableCollector.SSTableFilesAndRange> sourceSSTables;
        private long bundleUncompressedSize;
        private long bundleCompressedSize;
        private BundleNameGenerator bundleNameGenerator;

        Builder()
        {
            this.bundleManifest = new BundleManifest();
        }

        /**
         * Set the staging directory for all bundles
         * @param bundleStagingDirectory staging directory for all bundles
         * @return builder
         */
        public Builder bundleStagingDirectory(Path bundleStagingDirectory)
        {
            Preconditions.checkNotNull(bundleStagingDirectory, "Cannot set bundle staging directory to null");
            return with(b -> b.bundleStagingDirectory = bundleStagingDirectory);
        }

        /**
         * Set the bundle name generator
         * @param bundleNameGenerator generates bundle name
         * @return builder
         */
        public Builder bundleNameGenerator(BundleNameGenerator bundleNameGenerator)
        {
            return with(b -> b.bundleNameGenerator = bundleNameGenerator);
        }

        /**
         * Set the sequence of the bundle. It should be monotonically increasing
         * @param bundleSequence sequence of the bundle
         * @return builder
         */
        public Builder bundleSequence(int bundleSequence)
        {
            Preconditions.checkArgument(bundleSequence >= 0, "bundleSequence cannot be negative");
            return with(b -> b.bundleSequence = bundleSequence);
        }

        /**
         * Set the source sstables to be bundled
         * @param sourceSSTables sstables to be bundled
         * @return builder
         */
        public Builder sourceSSTables(List<SSTableCollector.SSTableFilesAndRange> sourceSSTables)
        {
            Preconditions.checkArgument(sourceSSTables != null && !sourceSSTables.isEmpty(),
                                        "No files to bundle");

            return with(b -> {
                b.sourceSSTables = sourceSSTables;
                b.bundleUncompressedSize = sourceSSTables.stream()
                                                         .mapToLong(sstable -> sstable.size)
                                                         .sum();
            });
        }

        public Bundle build()
        {
            try
            {
                prepareBuild();
            }
            catch (IOException ioe)
            {
                throw new RuntimeException("Unable to produce bundle manifest", ioe);
            }

            return new Bundle(this);
        }

        public Builder self()
        {
            return this;
        }

        private void prepareBuild() throws IOException
        {
            bundleDirectory = bundleStagingDirectory.resolve(Integer.toString(bundleSequence));
            Files.createDirectories(bundleDirectory);

            populateBundleManifestAndPersist();

            String bundleName = bundleNameGenerator.generate(startToken, endToken);
            bundleFile = bundleStagingDirectory.resolve(bundleName);
            bundleCompressedSize = SSTablesBundler.zip(bundleDirectory, bundleFile);
        }

        private void populateBundleManifestAndPersist() throws IOException
        {
            for (SSTableFilesAndRange sstable : sourceSSTables)
            {
                // all SSTable components related to one SSTable moved under same bundle
                BundleManifest.Entry manifestEntry = new BundleManifest.Entry(sstable.summary);
                for (Path componentPath : sstable.files)
                {
                    String checksum = IOUtils.xxhash32(componentPath);
                    Path targetPath = bundleDirectory.resolve(componentPath.getFileName());
                    // link the original files to the bundle dir to avoid copying data
                    Files.createLink(targetPath, componentPath);
                    manifestEntry.addComponentChecksum(componentPath.getFileName().toString(), checksum);
                }
                addManifestEntry(manifestEntry);
            }

            bundleManifest.persistTo(bundleDirectory.resolve(Bundle.MANIFEST_FILE_NAME));
        }

        private void addManifestEntry(BundleManifest.Entry entry)
        {
            if (bundleManifest.isEmpty())
            {
                startToken = entry.startToken();
                endToken = entry.endToken();
            }
            else
            {
                startToken = startToken.min(entry.startToken());
                endToken = endToken.max(entry.endToken());
            }
            bundleManifest.addEntry(entry);
        }
    }
}
