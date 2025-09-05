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

package org.apache.cassandra.bridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.security.EncryptionContext;

public class CassandraTypesImplementation extends AbstractCassandraTypes
{
    public static final CassandraTypesImplementation INSTANCE = new CassandraTypesImplementation();

    public static synchronized void setup()
    {
        if (!CassandraTypesImplementation.setup)
        {
            // We never want to enable mbean registration in the Cassandra code we use so disable it here
            System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
            Config.setClientMode(true);
            // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
            // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
            // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            DatabaseDescriptor.clientInitialization();
            Config config = DatabaseDescriptor.getRawConfig();
            config.memtable_flush_writers = 8;
            config.diagnostic_events_enabled = false;
            config.max_mutation_size_in_kb = config.commitlog_segment_size_in_mb * 1024 / 2;
            config.concurrent_compactors = 4;
            Path tempDirectory;
            try
            {
                tempDirectory = Files.createTempDirectory(UUID.randomUUID().toString());
            }
            catch (IOException exception)
            {
                throw new RuntimeException(exception);
            }
            config.data_file_directories = new String[]{tempDirectory.toString()};
            setupCommitLogConfigs(tempDirectory);
            DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
            Keyspace.setInitialized();
            setup = true;
        }
    }

    protected static void setupCommitLogConfigs(Path path)
    {
        Path commitLogPath = path.resolve("commitlog");
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath.toString();
        DatabaseDescriptor.getRawConfig().hints_directory = path.resolve("hints").toString();
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path.resolve("saved_caches").toString();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(32);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(commitLog -> new CommitLogSegmentManagerStandard(commitLog, commitLogPath.toString()));
    }
}
