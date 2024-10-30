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

import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.spark.data.FileSystemSource;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.utils.IOUtils;

public class LocalCommitLog implements CommitLog
{
    final long length;
    final String name;
    final Path path;
    FileSystemSource<CommitLog> source = null;
    final CassandraInstance instance;

    public LocalCommitLog(Path path)
    {
        this.name = path.getFileName().toString();
        this.path = path;
        this.length = IOUtils.size(path);
        this.instance = new CassandraInstance("0", "local-instance", "DC1");
    }

    public String name()
    {
        return name;
    }

    public String path()
    {
        return path.toString();
    }

    public long maxOffset()
    {
        return length;
    }

    public long length()
    {
        return length;
    }

    public boolean completed()
    {
        // for CDC Cassandra writes COMPLETED in the final line of the CommitLog-7-*_cdc.idx index file.
        return maxOffset() >= 67108818;
    }

    public FileSystemSource<CommitLog> source()
    {
        if (source != null)
        {
            return source;
        }

        try
        {
            this.source = new FileSystemSource<>(this, FileType.COMMITLOG, path);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return source;
    }

    public CassandraInstance instance()
    {
        return instance;
    }

    public void close() throws IOException
    {
        if (source != null)
        {
            source.close();
            source = null;
        }
    }
}
