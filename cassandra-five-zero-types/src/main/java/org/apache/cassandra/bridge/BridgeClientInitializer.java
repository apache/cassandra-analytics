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
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.SimpleSnitch;

/**
 * Performs the one-time client-mode initialization of the embedded Cassandra engine used by the bridge.
 *
 * <p>{@link #initialize(BridgeInitializationParameters)} here is the stock Apache Cassandra 5.0 sequence. The
 * client-init sequence is genuinely version-entangled — the snitch mechanism, whether the partitioner is set
 * before or after {@code clientInitialization}, and any cluster-metadata bootstrap — so a Cassandra
 * distribution whose client-init API differs simply subclasses this and overrides {@code initialize}, then
 * registers the subclass via {@link java.util.ServiceLoader} (a
 * {@code META-INF/services/org.apache.cassandra.bridge.BridgeClientInitializer} entry).
 * {@code CassandraTypesImplementation} loads the registered subclass, falling back to this default when none
 * is registered.</p>
 */
public class BridgeClientInitializer
{
    public void initialize(BridgeInitializationParameters params)
    {
        // We never want to enable mbean registration in the Cassandra code we use so disable it here
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
        System.setProperty("cassandra.schema.force_load_local_keyspaces", "true");
        Config.setClientMode(true);
        // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
        // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
        // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        Config config = new Config();
        config.memtable_flush_writers = 8;
        config.diagnostic_events_enabled = false;
        config.max_mutation_size = new DataStorageSpec.IntKibibytesBound(config.commitlog_segment_size.toKibibytes() / 2);
        config.concurrent_compactors = 4;
        config.sstable.selected_format = params.getConfiguredSSTableFormat();
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
        DatabaseDescriptor.clientInitialization(true, () -> config);
        CassandraTypesImplementation.setupCommitLogConfigs(tempDirectory);
        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
        Keyspace.setInitialized();
    }
}
