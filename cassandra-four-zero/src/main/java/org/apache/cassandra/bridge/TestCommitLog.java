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

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.spark.utils.IOUtils;

public class TestCommitLog implements CassandraBridge.ICommitLog
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestCommitLog.class);

    private final File folder;

    public TestCommitLog(File folder)
    {
        this.folder = folder;
        start();
    }

    @Override
    public synchronized void start()
    {
        LOGGER.info("Starting CommitLog");
        CommitLog.instance.start();
    }

    @Override
    public synchronized void stop()
    {
        try
        {
            LOGGER.info("Shutting down CommitLog");
            CommitLog.instance.shutdownBlocking();
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    @Override
    public synchronized void clear()
    {
        IOUtils.clearDirectory(folder.toPath(), path -> LOGGER.info("Deleting CommitLog: " + path.toString()));
    }

    @Override
    public void add(CassandraBridge.IMutation mutation)
    {
        if (mutation instanceof CassandraBridgeImplementation.MutationWrapper)
        {
            CommitLog.instance.add(((CassandraBridgeImplementation.MutationWrapper) mutation).mutation);
        }
    }

    @Override
    public void sync()
    {
        try
        {
            CommitLog.instance.sync(true);
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
