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

package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RowDataTests
{
    @Test
    public void testInit()
    {
        RowData rowData = new RowData();
        assertNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNull(rowData.getValue());
        assertFalse(rowData.isNewPartition);
    }

    @Test
    public void testSetPartitionKey()
    {
        RowData rowData = new RowData();
        rowData.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        assertNotNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNull(rowData.getValue());

        assertTrue(rowData.isNewPartition);
        assertTrue(rowData.isNewPartition());
        assertFalse(rowData.isNewPartition);
        assertEquals("101", toString(rowData.getPartitionKey()));

        rowData.setPartitionKeyCopy(ByteBuffer.wrap("102".getBytes()), BigInteger.ZERO);
        assertTrue(rowData.isNewPartition);
        assertTrue(rowData.isNewPartition());
        assertFalse(rowData.isNewPartition);
        assertEquals("102", toString(rowData.getPartitionKey()));
    }

    @Test
    public void testSetColumnKey()
    {
        RowData rowData = new RowData();
        assertNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNull(rowData.getValue());
        rowData.setColumnNameCopy(ByteBuffer.wrap("101".getBytes()));

        assertNull(rowData.getPartitionKey());
        assertNotNull(rowData.getColumnName());
        assertNull(rowData.getValue());
        assertEquals("101", toString(rowData.getColumnName()));
    }

    @Test
    public void testSetValue()
    {
        RowData rowData = new RowData();
        assertNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNull(rowData.getValue());
        rowData.setValueCopy(ByteBuffer.wrap("101".getBytes()));

        assertNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNotNull(rowData.getValue());
        assertEquals("101", toString(rowData.getValue()));
    }

    @Test
    public void testSetAll()
    {
        RowData rowData = new RowData();
        assertNull(rowData.getPartitionKey());
        assertNull(rowData.getColumnName());
        assertNull(rowData.getValue());
        rowData.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        rowData.setColumnNameCopy(ByteBuffer.wrap("102".getBytes()));
        rowData.setValueCopy(ByteBuffer.wrap("103".getBytes()));

        assertTrue(rowData.isNewPartition);
        assertTrue(rowData.isNewPartition());
        assertFalse(rowData.isNewPartition);

        assertNotNull(rowData.getPartitionKey());
        assertNotNull(rowData.getColumnName());
        assertNotNull(rowData.getValue());
        assertEquals("101", toString(rowData.getPartitionKey()));
        assertEquals("102", toString(rowData.getColumnName()));
        assertEquals("103", toString(rowData.getValue()));

        rowData.setPartitionKeyCopy(ByteBuffer.wrap("104".getBytes()), BigInteger.ZERO);
        assertTrue(rowData.isNewPartition);
        assertTrue(rowData.isNewPartition());
        assertFalse(rowData.isNewPartition);
        rowData.setColumnNameCopy(ByteBuffer.wrap("105".getBytes()));
        rowData.setValueCopy(ByteBuffer.wrap("106".getBytes()));
        assertEquals("104", toString(rowData.getPartitionKey()));
        assertEquals("105", toString(rowData.getColumnName()));
        assertEquals("106", toString(rowData.getValue()));
    }

    private static String toString(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
