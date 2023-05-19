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

package org.apache.cassandra.spark.data;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;

public class LocalCommitLog implements CommitLog
{
    final long length;
    final String name;
    final String path;
    final FileSystemSource source;
    final CassandraInstance instance;

    public LocalCommitLog(File file)
    {
        this.name = file.getName();
        this.path = file.getPath();
        this.length = file.length();
        this.instance = new CassandraInstance("0", "local-instance", "DC1");

        try
        {
            this.source = new FileSystemSource(null, FileType.COMMITLOG, file.toPath())
            {
                @Override
                public ExecutorService executor()
                {
                    return COMMIT_LOG_EXECUTOR;
                }

                @Override
                public long size()
                {
                    return length;
                }
            };
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String path()
    {
        return path;
    }

    @Override
    public long maxOffset()
    {
        return length;
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public SSTableSource<? extends SSTable> source()
    {
        return source;
    }

    @Override
    public CassandraInstance instance()
    {
        return instance;
    }

    @Override
    public void close() throws Exception
    {
        source.close();
    }
}
