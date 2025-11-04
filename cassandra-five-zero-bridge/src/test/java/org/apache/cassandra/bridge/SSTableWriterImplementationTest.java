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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for configuring {@link SSTableWriterImplementation}
 */
class SSTableWriterImplementationTest
{
    public static final String CREATE_STATEMENT = "CREATE TABLE test_keyspace.test_table (a int PRIMARY KEY, b text)";
    public static final String INSERT_STATEMENT = "INSERT INTO test_keyspace.test_table (a, b) VALUES (?, ?)";

    @TempDir
    File writeDirectory;

    static
    {
        CassandraTypesImplementation.setup(BridgeInitializationParameters.fromEnvironment());
    }

    @Test
    void testSSTableWriterConfiguration() throws NoSuchFieldException, IllegalAccessException
    {
        CQLSSTableWriter.Builder builder = SSTableWriterImplementation.configureBuilder(writeDirectory.getAbsolutePath(),
                                                                                        CREATE_STATEMENT,
                                                                                        INSERT_STATEMENT,
                                                                                        250,
                                                                                        Collections.emptySet(),
                                                                                        sstables -> {},
                                                                                        new Murmur3Partitioner());


        assertThat(peekSorted(builder)).isTrue();
        assertThat(peekBufferSizeInMB(builder)).isEqualTo(250);
    }

    @Test
    void testGetProducedSSTables() throws IOException
    {
        Set<SSTableDescriptor> produced = new HashSet<>();
        SSTableWriterImplementation writer = new SSTableWriterImplementation(writeDirectory.getAbsolutePath(),
                                                                             new ByteOrderedPartitioner(), // required in order to insert ordered ints
                                                                             CREATE_STATEMENT,
                                                                             INSERT_STATEMENT,
                                                                             Collections.emptySet(),
                                                                             1);
        writer.setSSTablesProducedListener(produced::addAll);
        assertThat(produced).isEmpty();

        for (int i = 0; i < 300_000; i++)
        {
            writer.addRow(ImmutableMap.of("a", i, "b", "val_" + i));
        }

        assertThat(produced).hasSize(2);
        String sstableFormat = DatabaseDescriptor.getRawConfig().sstable.selected_format;
        assertThat(sstableFormat).containsAnyOf("big", "bti");
        String sstableVersion = "bti".equals(sstableFormat) ? "da" : "oa";
        assertThat(produced.stream().map(e -> e.baseFilename)).containsExactlyInAnyOrder(
            toSStableFileName(sstableFormat, sstableVersion, 2),
            toSStableFileName(sstableFormat, sstableVersion, 3));
        produced.clear();

        for (int i = 300_000; i < 400_000; i++)
        {
            writer.addRow(ImmutableMap.of("a", i, "b", "val_" + i));
        }
        assertThat(produced.size()).isEqualTo(1);
        assertThat(produced.stream().map(e -> e.baseFilename))
            .containsExactly(toSStableFileName(sstableFormat, sstableVersion, 4));

        // when closing the writer, a new sstable is produced (by flushing the remaining data in the buffer)
        produced.clear();
        writer.close();
        assertThat(produced.size()).isEqualTo(1);
        assertThat(produced.stream().map(e -> e.baseFilename))
            .containsExactly(toSStableFileName(sstableFormat, sstableVersion, 5));
    }

    @Test
    void testBaseFileNameExtraction()
    {
        org.apache.cassandra.io.util.File cf = new org.apache.cassandra.io.util.File(writeDirectory);
        SSTableId ssTableId = new SequenceBasedSSTableId(1);
        Descriptor descriptor = new Descriptor("nb", cf, "ks", "tbl", ssTableId, TestUtils.BIG_FORMAT);
        assertThat(CassandraBridgeImplementation.baseFilename(descriptor)).isEqualTo("nb-1-big");
    }

    static boolean peekSorted(CQLSSTableWriter.Builder builder) throws NoSuchFieldException, IllegalAccessException
    {
        Field sortedField = ReflectionUtils.getField(builder.getClass(), "sorted");
        sortedField.setAccessible(true);
        return (boolean) sortedField.get(builder);
    }

    static long peekBufferSizeInMB(CQLSSTableWriter.Builder builder) throws NoSuchFieldException, IllegalAccessException
    {
        // The name of the size field has been changed in Cassandra code base.
        // We find the field using the old name to newer one.
        Field sizeField = findFirstField(builder.getClass(),
                                         "bufferSizeInMB", "bufferSizeInMiB", "maxSSTableSizeInMiB");
        sizeField.setAccessible(true);
        return (long) sizeField.get(builder);
    }

    static Field findFirstField(Class<?> clazz, String... fieldNames) throws NoSuchFieldException
    {
        Field field = null;
        for (String fieldName : fieldNames)
        {
            try
            {
                field = ReflectionUtils.getField(clazz, fieldName);
            }
            catch (NoSuchFieldException nsfe)
            {
                // ignore the exception and try with the next fieldName
            }
        }

        if (field == null)
        {
            throw new NoSuchFieldException("The class does not contain any of the supplied fieldNames: " + Arrays.asList(fieldNames));
        }

        return field;
    }

    private void waitForProduced(Set<SSTableDescriptor> produced)
    {
        int i = 15; // the test runs roughly within 2 seconds; 3_000 milliseconds timeout should suffice.
        while (produced.isEmpty() && i-- > 0)
        {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
        }
    }

    private String toSStableFileName(String format, String version, int number)
    {
        return String.format("%s-%d-%s", version, number, format);
    }
}
