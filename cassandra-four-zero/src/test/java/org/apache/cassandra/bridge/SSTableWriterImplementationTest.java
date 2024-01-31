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
import java.lang.reflect.Field;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.utils.ReflectionUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for configuring {@link SSTableWriterImplementation}
 */
class SSTableWriterImplementationTest
{
    public static final String CREATE_STATEMENT = "CREATE TABLE test_keyspace.test_table (a int, b text)";
    public static final String INSERT_STATEMENT = "INSERT INTO test_keyspace.test_table (a, b) VALUES (?, ?)";

    @TempDir
    File writeDirectory;

    @Test
    void testSSTableWriterConfiguration() throws NoSuchFieldException, IllegalAccessException
    {
        CQLSSTableWriter.Builder builder = SSTableWriterImplementation.configureBuilder(writeDirectory.getAbsolutePath(),
                                                                                        CREATE_STATEMENT,
                                                                                        INSERT_STATEMENT,
                                                                                        250,
                                                                                        new Murmur3Partitioner());


        assertTrue(peekSorted(builder));
        assertEquals(250, peekBufferSizeInMB(builder));
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

    static Field findFirstField(Class<?> clazz, String... fieldNames) throws NoSuchFieldException, IllegalAccessException
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
}
