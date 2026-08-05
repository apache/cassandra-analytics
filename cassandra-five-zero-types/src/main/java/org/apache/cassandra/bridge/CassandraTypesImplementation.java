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

import java.nio.file.Path;
import java.util.ServiceLoader;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.complex.CqlVector;

public class CassandraTypesImplementation extends AbstractCassandraTypes
{
    public static final CassandraTypesImplementation INSTANCE = new CassandraTypesImplementation();

    public static synchronized void setup(BridgeInitializationParameters params)
    {
        if (!CassandraTypesImplementation.setup)
        {
            // Client-mode engine initialization is version-specific (a distribution may change the snitch
            // mechanism, the partitioner ordering, or add cluster-metadata bootstrap), so it is delegated to a
            // BridgeClientInitializer discovered via ServiceLoader; absent a registered one, the stock Apache
            // default is used. The synchronized one-time guard stays here.
            resolveClientInitializer().initialize(params);
            setup = true;
        }
    }

    private static BridgeClientInitializer resolveClientInitializer()
    {
        return ServiceLoader.load(BridgeClientInitializer.class, BridgeClientInitializer.class.getClassLoader())
                            .findFirst()
                            .orElseGet(BridgeClientInitializer::new);
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
        DatabaseDescriptor.getRawConfig().commitlog_total_space = new DataStorageSpec.IntMebibytesBound(1024);
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(commitLog -> new CommitLogSegmentManagerStandard(commitLog, commitLogPath.toString()));
    }

    @Override
    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        if (type == CqlField.CqlType.InternalType.Vector)
        {
            return CqlVector.read(input, this);
        }
        return super.readType(type, input);
    }

    @Override
    public CqlField.CqlVector vector(CqlField.CqlType type, int dimensions)
    {
        return new CqlVector(type, dimensions);
    }
}
