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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.SSTables;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * <br>
 * <p>Threading Model:</p>
 * This class has limited thread-safety guarantees:
 * <ul>
 *   <li>{@link #addRow(BigInteger, Map)} and {@link #close(BulkWriterContext)} MUST be called from the same thread
 *       (typically the RecordWriter thread). These methods are NOT synchronized and must not be called concurrently.</li>
 *   <li>{@link #prepareSStablesToSend(BulkWriterContext, Set)} MAY be called concurrently from background threads
 *       (via {@link StreamSession}'s executor service) and is synchronized to protect shared state.</li>
 *   <li>{@link #close(BulkWriterContext)} is synchronized to prevent races with concurrent
 *       {@link #prepareSStablesToSend(BulkWriterContext, Set)} calls.</li>
 *   <li>Getter methods ({@link #rowCount()}, {@link #bytesWritten()}, {@link #sstableCount()}) may return stale
 *       values if called concurrently with {@link #prepareSStablesToSend(BulkWriterContext, Set)} or
 *       {@link #close(BulkWriterContext)}. They are only guaranteed accurate after {@link #close(BulkWriterContext)}
 *       completes.</li>
 * </ul>
 */
@SuppressWarnings("WeakerAccess")
public class SortedSSTableWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SortedSSTableWriter.class);

    public static final String CASSANDRA_VERSION_PREFIX = "cassandra-";

    private final Path outDir;
    private final org.apache.cassandra.bridge.SSTableWriter cqlSSTableWriter;
    private final int partitionId;
    private final DigestAlgorithm digestAlgorithm;

    // Fields accessed only from the RecordWriter thread (addRow/close caller)
    private BigInteger minToken = null;
    private BigInteger maxToken = null;
    private long rowCount = 0;

    // Fields protected by synchronization - accessed from both RecordWriter thread and executor threads
    private final Map<Path, Digest> overallFileDigests = new HashMap<>();
    private boolean isClosed = false;
    private int sstableCount = 0;
    private long bytesWritten = 0;

    public SortedSSTableWriter(org.apache.cassandra.bridge.SSTableWriter tableWriter, Path outDir,
                               DigestAlgorithm digestAlgorithm,
                               int partitionId)
    {
        this.cqlSSTableWriter = tableWriter;
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;
        this.partitionId = partitionId;
    }

    public SortedSSTableWriter(BulkWriterContext writerContext, Path outDir, DigestAlgorithm digestAlgorithm, int partitionId)
    {
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;
        this.partitionId = partitionId;

        String bridgeVersion = writerContext.cluster().getBridgeVersion();
        String packageVersion = getPackageVersion(bridgeVersion);
        LOGGER.info("Running with version {}", packageVersion);

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
    public String getPackageVersion(String bridgeVersion)
    {
        return CASSANDRA_VERSION_PREFIX + bridgeVersion;
    }

    /**
     * Add a row to be written.
     * <p>
     * <b>Threading:</b> This method MUST be called from the same thread that calls {@link #close(BulkWriterContext)}
     * (typically the RecordWriter thread). It is NOT thread-safe and must not be called concurrently with any other
     * method on this instance.
     *
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

    public void setSSTablesProducedListener(Consumer<Set<SSTableDescriptor>> listener)
    {
        cqlSSTableWriter.setSSTablesProducedListener(listener);
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

    /**
     * Prepares a set of SSTables to be sent to replicas by calculating digests and validating them.
     * <p>
     * This method is called when SSTables are produced during the write process (before final close).
     * It processes newly-produced SSTables, calculates their file digests, validates them, and updates
     * the internal counters.
     * <p>
     * <b>Threading:</b> This method is thread-safe and may be called concurrently from background threads
     * (e.g., from {@link DirectStreamSession#onSSTablesProduced(Set)} via the executor service).
     * It is synchronized to protect shared state ({@code overallFileDigests}, {@code sstableCount},
     * {@code bytesWritten}) from concurrent access with {@link #close(BulkWriterContext)}.
     *
     * @param writerContext the bulk writer context
     * @param sstables the set of SSTable descriptors to prepare
     * @return a map of file paths to their digests, or an empty map if the writer is already closed
     * @throws IOException if an I/O error occurs
     */
    public synchronized Map<Path, Digest> prepareSStablesToSend(@NotNull BulkWriterContext writerContext, Set<SSTableDescriptor> sstables) throws IOException
    {
        // If the writer is already closed, return empty map
        // The remaining SSTables will be handled by sendRemainingSSTables()
        if (isClosed)
        {
            LOGGER.debug("Writer is already closed, returning empty digest map. Remaining SSTables will be handled by sendRemainingSSTables()");
            return Collections.emptyMap();
        }

        // Filter for SSTables that match the requested descriptors AND haven't been hashed yet
        DirectoryStream.Filter<Path> sstableFilter = path -> {
            SSTableDescriptor baseName = SSTables.getSSTableDescriptor(path);
            return sstables.contains(baseName) && !overallFileDigests.containsKey(path);
        };
        Set<Path> dataFilePaths = new HashSet<>();
        Map<Path, Digest> fileDigests = new HashMap<>();
        // FIXME: CQLSSTableWriter may produce incomplete Filter.db file, rebuilding it manually (see CASSANDRA-21423).
        // rebuild Filter.db files before calculating their digest
        rebuildFilterComponents(writerContext, sstableFilter);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getOutDir(), sstableFilter))
        {
            for (Path path : stream)
            {
                if (path.getFileName().toString().endsWith("-" + FileType.DATA.getFileSuffix()))
                {
                    dataFilePaths.add(path);
                    sstableCount += 1;
                }

                Digest digest = digestAlgorithm.calculateFileDigest(path);
                fileDigests.put(path, digest);
                LOGGER.debug("Calculated digest={} for path={}", digest, path);
            }
        }
        bytesWritten += calculatedTotalSize(fileDigests.keySet());
        overallFileDigests.putAll(fileDigests);
        validateSSTables(writerContext, getOutDir(), dataFilePaths);
        return fileDigests;
    }

    /**
     * Closes this writer, flushes any remaining data, calculates digests, and validates all SSTables.
     * <p>
     * This method performs the final flush of the SSTable writer, processes any SSTables that were not
     * already handled by {@link #prepareSStablesToSend(BulkWriterContext, Set)}, calculates their digests,
     * and validates all written SSTables.
     * <p>
     * <b>Threading:</b> This method MUST be called from the same thread that calls {@link #addRow(BigInteger, Map)}
     * (typically the RecordWriter thread). It is synchronized to prevent races with concurrent
     * {@link #prepareSStablesToSend(BulkWriterContext, Set)} calls from background threads.
     * <p>
     * This method is idempotent - calling it multiple times will return early after the first call completes.
     *
     * @param writerContext the bulk writer context
     * @throws IOException if an I/O error occurs during closing
     */
    public synchronized void close(BulkWriterContext writerContext) throws IOException
    {
        if (isClosed)
        {
            LOGGER.info("Already closed");
            return;
        }
        isClosed = true;
        cqlSSTableWriter.close();

        Set<Path> hashedFiles = new HashSet<>(overallFileDigests.keySet());
        Set<Path> newlyHashedFiles = new HashSet<>();

        // Filter out SSTables that were already hashed during production (via prepareSStablesToSend)
        // Only process new SSTables produced during final flush
        DirectoryStream.Filter<Path> unhashedFilter = path -> !hashedFiles.contains(path);

        // FIXME: CQLSSTableWriter may produce incomplete Filter.db file, rebuilding it manually (see CASSANDRA-21423).
        rebuildFilterComponents(writerContext, unhashedFilter);

        try (DirectoryStream<Path> dataFileStream = getDataFileStream(unhashedFilter))
        {
            for (Path dataFile : dataFileStream)
            {
                // NOTE: We calculate file hashes before re-reading so that we know what we hashed
                //       is what we validated. Then we send these along with the files and the
                //       receiving end re-hashes the files to make sure they still match.
                Map<Path, Digest> newFileDigests = calculateFileDigestMap(dataFile);
                overallFileDigests.putAll(newFileDigests);
                newlyHashedFiles.addAll(newFileDigests.keySet());
                sstableCount += 1;
            }
        }
        // Only calculate size for newly hashed files, not all files in overallFileDigests
        // (previously hashed files may have been deleted by prepareSStablesToSend)
        bytesWritten += calculatedTotalSize(newlyHashedFiles);
        validateSSTables(writerContext);
    }

    protected void rebuildFilterComponents(@NotNull BulkWriterContext writerContext,
                                           @NotNull DirectoryStream.Filter<Path> filter) throws IOException
    {
        LocalDataLayer layer = buildLocalDataLayer(writerContext, getOutDir(), null);
        try (DirectoryStream<Path> dataFileStream = getDataFileStream(filter))
        {
            for (Path dataFile : dataFileStream)
            {
                FileSystemSSTable ssTable = new FileSystemSSTable(dataFile, false, BufferingInputStreamStats::doNothingStats);
                writerContext.bridge().rebuildBloomFilter(layer.partitioner(), layer.cqlTable(), ssTable, getOutDir());
                LOGGER.debug("Rebuilt bloom filter for sstable {}", dataFile);
            }
        }
    }

    @VisibleForTesting
    public void validateSSTables(@NotNull BulkWriterContext writerContext)
    {
        validateSSTables(writerContext, getOutDir(), null);
    }

    /**
     * Validate SSTables. If dataFilePaths is null, it finds all sstables under the output directory of the writer and validates them
     *
     * @param outputDirectory output directory of the sstable writer
     * @param writerContext bulk writer context
     * @param dataFilePaths paths of sstables (data file) to be validated. The argument is nullable.
     *                      When it is null, it validates all sstables under the output directory.
     */
    @VisibleForTesting
    public void validateSSTables(@NotNull BulkWriterContext writerContext, @NotNull Path outputDirectory, @Nullable Set<Path> dataFilePaths)
    {
        // NOTE: If this current implementation of SS-tables' validation proves to be a performance issue,
        //       we will need to modify LocalDataLayer to allow scanning and compaction of single data file,
        //       and then validate all of them in parallel threads
        try
        {
            LocalDataLayer layer = buildLocalDataLayer(writerContext, outputDirectory, dataFilePaths);
            try (StreamScanner<RowData> scanner = layer.openCompactionScanner(partitionId, Collections.emptyList(), null))
            {
                while (scanner.next())
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

    private LocalDataLayer buildLocalDataLayer(@NotNull BulkWriterContext writerContext, @NotNull Path outputDirectory, @Nullable Set<Path> dataFilePaths)
    {
        CassandraVersion version = CassandraBridgeFactory.getCassandraVersion(writerContext.cluster().getBridgeVersion());
        String keyspace = writerContext.job().qualifiedTableName().keyspace();
        String schema = writerContext.schema().getTableSchema().createStatement;
        Partitioner partitioner = writerContext.cluster().getPartitioner();
        Set<String> udtStatements = writerContext.schema().getUserDefinedTypeStatements();
        LocalDataLayer layer = new LocalDataLayer(version,
                                                  partitioner,
                                                  keyspace,
                                                  schema,
                                                  udtStatements,
                                                  Collections.emptyList() /* requestedFeatures */,
                                                  false /* useSSTableInputStream */,
                                                  null /* statsClass */,
                                                  SSTableTimeRangeFilter.ALL,
                                                  outputDirectory.toString());
        if (dataFilePaths != null)
        {
            layer.setDataFilePaths(dataFilePaths);
        }
        return layer;
    }

    private DirectoryStream<Path> getDataFileStream(DirectoryStream.Filter<Path> filter) throws IOException
    {
        // Combine the data file filter with the provided filter
        DirectoryStream.Filter<Path> combinedFilter = path -> {
            String fileName = path.getFileName().toString();
            return fileName.endsWith("Data.db") && filter.accept(path);
        };
        return Files.newDirectoryStream(getOutDir(), combinedFilter);
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
        return Collections.unmodifiableMap(overallFileDigests);
    }
}
