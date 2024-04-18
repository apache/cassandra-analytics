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
import java.math.BigInteger;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.SSTables;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.jetbrains.annotations.NotNull;

/**
 * SSTableWriter that expects sorted data
 * <br>
 * Note for implementor: the bulk writer always sort the data in entire spark partition before writing. One of the
 * benefit is that the output sstables are sorted and non-overlapping. It allows Cassandra to perform optimization
 * when importing those sstables, as they can be considered as a single large SSTable technically.
 * You might want to introduce a SSTableWriter for unsorted data, say UnsortedSSTableWriter, and stop sorting the
 * entire partition, i.e. repartitionAndSortWithinPartitions. By doing so, it eliminates the nice property of the
 * output sstable being globally sorted and non-overlapping.
 * Unless you can think of a better use case, we should stick with this SortedSSTableWriter
 */
@SuppressWarnings("WeakerAccess")
public class SortedSSTableWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SortedSSTableWriter.class);

    public static final String CASSANDRA_VERSION_PREFIX = "cassandra-";

    private final Path outDir;
    private final org.apache.cassandra.bridge.SSTableWriter cqlSSTableWriter;
    private BigInteger minToken = null;
    private BigInteger maxToken = null;
    private final Map<Path, Digest> fileDigestMap = new HashMap<>();
    private final DigestAlgorithm digestAlgorithm;

    private int sstableCount = 0;
    private long rowCount = 0;
    private long bytesWritten = 0;

    public SortedSSTableWriter(org.apache.cassandra.bridge.SSTableWriter tableWriter, Path outDir,
                               DigestAlgorithm digestAlgorithm)
    {
        cqlSSTableWriter = tableWriter;
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;
    }

    public SortedSSTableWriter(BulkWriterContext writerContext, Path outDir, DigestAlgorithm digestAlgorithm)
    {
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;

        String lowestCassandraVersion = writerContext.cluster().getLowestCassandraVersion();
        String packageVersion = getPackageVersion(lowestCassandraVersion);
        LOGGER.info("Running with version " + packageVersion);

        SchemaInfo schema = writerContext.schema();
        TableSchema tableSchema = schema.getTableSchema();
        this.cqlSSTableWriter = SSTableWriterFactory.getSSTableWriter(
        CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(packageVersion),
        this.outDir.toString(),
        writerContext.cluster().getPartitioner().toString(),
        tableSchema.createStatement,
        tableSchema.modificationStatement,
        schema.getUserDefinedTypeStatements(),
        writerContext.job().sstableDataSizeInMiB());
    }

    @NotNull
    public String getPackageVersion(String lowestCassandraVersion)
    {
        return CASSANDRA_VERSION_PREFIX + lowestCassandraVersion;
    }

    /**
     * Add a row to be written.
     * @param token the hashed token of the row's partition key.
     *              The value must be monotonically increasing in the subsequent calls.
     * @param boundValues bound values of the columns in the row
     * @throws IOException I/O exception when adding the row
     */
    public void addRow(BigInteger token, Map<String, Object> boundValues) throws IOException
    {
        if (rowCount == 0)
        {
            minToken = token;
        }
        // rows are sorted. Therefore, only update the maxToken
        maxToken = token;
        cqlSSTableWriter.addRow(boundValues);
        rowCount += 1;
    }

    /**
     * @return the total number of rows written
     */
    public long rowCount()
    {
        return rowCount;
    }

    /**
     * @return the total number of bytes written
     */
    public long bytesWritten()
    {
        return bytesWritten;
    }

    /**
     * @return the total number of sstables written
     */
    public int sstableCount()
    {
        return sstableCount;
    }

    public void close(BulkWriterContext writerContext, int partitionId) throws IOException
    {
        cqlSSTableWriter.close();
        sstableCount = 0;
        for (Path dataFile : getDataFileStream())
        {
            // NOTE: We calculate file hashes before re-reading so that we know what we hashed
            //       is what we validated. Then we send these along with the files and the
            //       receiving end re-hashes the files to make sure they still match.
            fileDigestMap.putAll(calculateFileDigestMap(dataFile));
            sstableCount += 1;
        }
        bytesWritten = calculatedTotalSize(fileDigestMap.keySet());
        validateSSTables(writerContext, partitionId);
    }

    @VisibleForTesting
    public void validateSSTables(@NotNull BulkWriterContext writerContext, int partitionId)
    {
        // NOTE: If this current implementation of SS-tables' validation proves to be a performance issue,
        //       we will need to modify LocalDataLayer to allow scanning and compaction of single data file,
        //       and then validate all of them in parallel threads
        try
        {
            CassandraVersion version = CassandraBridgeFactory.getCassandraVersion(writerContext.cluster().getLowestCassandraVersion());
            String keyspace = writerContext.job().keyspace();
            String schema = writerContext.schema().getTableSchema().createStatement;
            Set<String> udtStatements = writerContext.schema().getUserDefinedTypeStatements();
            String directory = getOutDir().toString();
            DataLayer layer = new LocalDataLayer(version, keyspace, schema, udtStatements, directory);
            try (StreamScanner<Rid> scanner = layer.openCompactionScanner(partitionId, Collections.emptyList(), null))
            {
                while (scanner.hasNext())
                {
                    scanner.advanceToNextColumn();
                }
            }
        }
        catch (IOException exception)
        {
            LOGGER.error("[{}]: Unexpected exception while validating SSTables {}", partitionId, getOutDir());
            throw new RuntimeException(exception);
        }
    }

    private DirectoryStream<Path> getDataFileStream() throws IOException
    {
        return Files.newDirectoryStream(getOutDir(), "*Data.db");
    }

    private Map<Path, Digest> calculateFileDigestMap(Path dataFile) throws IOException
    {
        Map<Path, Digest> fileHashes = new HashMap<>();
        try (DirectoryStream<Path> filesToHash =
             Files.newDirectoryStream(dataFile.getParent(), SSTables.getSSTableBaseName(dataFile) + "*"))
        {
            for (Path path : filesToHash)
            {
                Digest digest = digestAlgorithm.calculateFileDigest(path);
                fileHashes.put(path, digest);
                LOGGER.debug("Calculated digest={} for path={}", digest, path);
            }
        }
        return fileHashes;
    }

    private long calculatedTotalSize(Collection<Path> paths) throws IOException
    {
        long totalSize = 0;
        for (Path path : paths)
        {
            totalSize += Files.size(path);
        }
        return totalSize;
    }

    public Range<BigInteger> getTokenRange()
    {
        return Range.closed(minToken, maxToken);
    }

    public Path getOutDir()
    {
        return outDir;
    }

    /**
     * @return a view of the file digest map
     */
    public Map<Path, Digest> fileDigestMap()
    {
        return Collections.unmodifiableMap(fileDigestMap);
    }
}
