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
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class SSTableWriterImplementation implements SSTableWriter
{
    static
    {
        Config.setClientMode(true);
    }

    private final CQLSSTableWriter writer;

    public SSTableWriterImplementation(String inDirectory,
                                       String partitioner,
                                       String createStatement,
                                       String insertStatement,
                                       boolean isSorted,
                                       int bufferSizeMB)
    {
        IPartitioner cassPartitioner = partitioner.toLowerCase().contains("random") ? new RandomPartitioner()
                                                                                    : new Murmur3Partitioner();
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .inDirectory(inDirectory)
                                                           .forTable(createStatement)
                                                           .withPartitioner(cassPartitioner)
                                                           .using(insertStatement)
                                                           .withBufferSizeInMB(bufferSizeMB);
        if (isSorted)
        {
            builder.sorted();
        }
        // TODO: Remove me once CQLSSTableWriter.Builder synchronize on schema (see CASSANDRA-TBD)
        //       build update schema, we need to synchronize
        writer = CassandraSchema.apply(s -> builder.build());
    }

    @Override
    public void addRow(Map<String, Object> values)  throws IOException
    {
        try
        {
            writer.addRow(values);
        }
        catch (InvalidRequestException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
    }
}
