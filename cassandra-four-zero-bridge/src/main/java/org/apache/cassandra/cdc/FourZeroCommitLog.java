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

package org.apache.cassandra.cdc;

import java.io.IOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.CommitLogInstance;
import org.apache.cassandra.cdc.api.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.spark.utils.IOUtils;

public class FourZeroCommitLog implements CommitLogInstance
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FourZeroCommitLog.class);
    private final Path directory;

    public FourZeroCommitLog(Path folder)
    {
        this.directory = folder;
        this.start();
    }

    @Override
    public synchronized void start()
    {
        LOGGER.info("Starting CommitLog.");
        CommitLog.instance.start();
    }

    @Override
    public synchronized void stop()
    {
        try
        {
            LOGGER.info("Shutting down CommitLog.");
            CommitLog.instance.stopUnsafe(true);
            CommitLog.instance.shutdownBlocking();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void clear()
    {
        IOUtils.clearDirectory(directory, (path) -> LOGGER.info("Deleting CommitLog: " + path.toString()));
    }

    @Override
    public void add(Mutation mutation)
    {
        if (mutation instanceof FourZeroMutation)
        {
            add(((FourZeroMutation) mutation).mutation);
        }
    }

    public void add(org.apache.cassandra.db.Mutation mutation)
    {
        CommitLog.instance.add(mutation);
    }

    @Override
    public void sync()
    {
        try
        {
            CommitLog.instance.sync(true);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
