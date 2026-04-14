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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.security.EncryptionContext;

public class CdcBridgeImplementation extends AbstractCdcBridgeImplementation
{
    public static volatile boolean setup = false;

    public static void setup(Path path, int commitLogSegmentSize, boolean enableCompression, BridgeInitializationParameters bridgeParams)
    {
        CassandraTypesImplementation.setup(bridgeParams);
        setCDC(path, commitLogSegmentSize, enableCompression);
    }

    public CdcBridgeImplementation()
    {
    }

    protected static synchronized void setCDC(Path path, int commitLogSegmentSize, boolean enableCompression)
    {
        if (setup)
        {
            return;
        }
        Path commitLogPath = path.resolve("commitlog");
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath.toString();
        DatabaseDescriptor.getRawConfig().hints_directory = path.resolve("hints").toString();
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path.resolve("saved_caches").toString();
        DatabaseDescriptor.getRawConfig().cdc_raw_directory = path.resolve("cdc").toString();
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        if (enableCompression)
        {
            DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        }
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(commitLogSegmentSize);
        DatabaseDescriptor.getRawConfig().commitlog_total_space = new DataStorageSpec.IntMebibytesBound(1024);
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
        DatabaseDescriptor.setCDCTotalSpaceInMiB(1024);
        DatabaseDescriptor.setCommitLogSegmentMgrProvider((commitLog -> new CommitLogSegmentManagerCDC(commitLog, commitLogPath.toString())));
        setup = true;
    }
}
