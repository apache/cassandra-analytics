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

package org.apache.cassandra.cdc.test;

import java.nio.file.Path;

import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.msg.jdk.JdkMessageConverter;

/**
 * Base class for CDC tests. All fields are initialized before test execution if given method
 * is a parameterized test with {@code CassandraVersion} parameter.
 */
@ExtendWith(CdcBridgeTestInjector.class)
public abstract class CdcTestBase
{
    protected CdcOptions cdcOptions;
    protected CassandraBridge bridge;
    protected CdcBridge cdcBridge;
    protected JdkMessageConverter messageConverter;
    protected CommitLogInstance commitLog;
    protected Path commitLogDir;

    void setup(CassandraVersion version)
    {
        this.cdcOptions = CdcBridgeProvider.getCdcOptions(version);
        this.bridge = CdcBridgeProvider.getCassandraBridge(version);
        this.cdcBridge = CdcBridgeProvider.getTestCdcBridge(version);
        this.commitLogDir = CdcBridgeProvider.getCommitLogDir(version);
        this.commitLog = cdcBridge.createCommitLogInstance(commitLogDir);
        this.messageConverter = CdcBridgeProvider.getMessageConverter(version);
    }
}
