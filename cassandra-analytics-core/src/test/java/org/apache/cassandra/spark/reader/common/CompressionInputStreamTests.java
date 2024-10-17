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

package org.apache.cassandra.spark.reader.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.analytics.reader.common.RawInputStream;
import org.apache.cassandra.analytics.stats.Stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompressionInputStreamTests
{
    @Test
    public void testRawInputStream() throws IOException
    {
        String filename = UUID.randomUUID().toString();
        Path path = Files.createTempFile(filename, "tmp");
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile()))))
        {
            dos.writeUTF(filename);
            int numWrites = 1000;
            dos.writeInt(numWrites);
            for (int write = 0; write < numWrites; write++)
            {
                dos.writeInt(write);
            }
            for (int write = 0; write < numWrites; write++)
            {
                dos.writeLong(write);
            }
            dos.writeBoolean(false);
            dos.writeBoolean(true);
        }

        byte[] buffer = new byte[1024];
        try (DataInputStream dis = new DataInputStream(new RawInputStream(new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path))), buffer, Stats.DoNothingStats.INSTANCE)))
        {
            assertEquals(filename, dis.readUTF());
            int numReads = dis.readInt();
            for (int read = 0; read < numReads; read++)
            {
                assertEquals(read, dis.readInt());
            }
            for (int read = 0; read < numReads; read++)
            {
                assertEquals(read, dis.readLong());
            }
            assertFalse(dis.readBoolean());
            assertTrue(dis.readBoolean());
        }
    }

    @Test()
    public void testBigLongArrayIllegalSize()
    {
        assertThrows(IndexOutOfBoundsException.class,
                     () -> new BigLongArray(-1)
        );
    }

    @Test()
    public void testBigLongArrayEmpty()
    {
        assertThrows(IndexOutOfBoundsException.class,
                     () -> {
                         BigLongArray array = new BigLongArray(0);
                         array.set(0, 0L);
                     });
    }

    @Test()
    public void testBigLongArrayOutOfRange()
    {
        assertThrows(IndexOutOfBoundsException.class,
                     () -> {
                         BigLongArray array = new BigLongArray(500);
                         array.set(501, 999L);
                     });
    }

    @Test
    public void testBigLongArrayUnary()
    {
        BigLongArray array = new BigLongArray(1);
        array.set(0, 999L);
        assertEquals(999L, array.get(0));
    }

    @Test
    public void testBigLongArray()
    {
        int size = BigLongArray.DEFAULT_PAGE_SIZE * 37;
        BigLongArray array = new BigLongArray(size);
        for (int index = 0; index < size; index++)
        {
            array.set(index, index * 5L);
        }
        for (int index = 0; index < size; index++)
        {
            assertEquals(index * 5L, array.get(index));
        }
    }
}
