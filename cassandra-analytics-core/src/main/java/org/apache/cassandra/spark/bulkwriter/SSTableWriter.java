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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

@SuppressWarnings("WeakerAccess")
public class SSTableWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriter.class);

    public static final String CASSANDRA_VERSION_PREFIX = "cassandra-";

    private final Path outDir;
    private final org.apache.cassandra.bridge.SSTableWriter cqlSSTableWriter;
    private BigInteger minToken = null;
    private BigInteger maxToken = null;
    private final Map<Path, Digest> fileDigestMap = new HashMap<>();
    private final DigestAlgorithm digestAlgorithm;

    private long rowCount = 0;


    public SSTableWriter(org.apache.cassandra.bridge.SSTableWriter tableWriter, Path outDir,
                         DigestAlgorithm digestAlgorithm)
    {
        cqlSSTableWriter = tableWriter;
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;
    }

    public SSTableWriter(BulkWriterContext writerContext, Path outDir, DigestAlgorithm digestAlgorithm)
    {
        this.outDir = outDir;
        this.digestAlgorithm = digestAlgorithm;

        String lowestCassandraVersion = writerContext.cluster().getLowestCassandraVersion();
        String packageVersion = getPackageVersion(lowestCassandraVersion);
        LOGGER.info("Running with version " + packageVersion);

        TableSchema tableSchema = writerContext.schema().getTableSchema();
        this.cqlSSTableWriter = SSTableWriterFactory.getSSTableWriter(
        CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(packageVersion),
        this.outDir.toString(),
        writerContext.cluster().getPartitioner().toString(),
        tableSchema.createStatement,
        tableSchema.modificationStatement,
        writerContext.job().sstableDataSizeInMiB());
    }

    @NotNull
    public String getPackageVersion(String lowestCassandraVersion)
    {
        return CASSANDRA_VERSION_PREFIX + lowestCassandraVersion;
    }

    public void addRow(BigInteger token, Map<String, Object> boundValues) throws IOException
    {
        if (minToken == null)
        {
            minToken = token;
        }
        maxToken = token;
        cqlSSTableWriter.addRow(boundValues);
        rowCount += 1;
    }

    /**
     * @return the total number of rows added
     */
    public long rowCount()
    {
        return rowCount;
    }

    public void close(BulkWriterContext writerContext, int partitionId) throws IOException
    {
        cqlSSTableWriter.close();
        for (Path dataFile : getDataFileStream())
        {
            // NOTE: We calculate file hashes before re-reading so that we know what we hashed
            //       is what we validated. Then we send these along with the files and the
            //       receiving end re-hashes the files to make sure they still match.
            fileDigestMap.putAll(calculateFileDigestMap(dataFile));
        }
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
            String directory = getOutDir().toString();
            DataLayer layer = new LocalDataLayer(version, keyspace, schema, directory);
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
