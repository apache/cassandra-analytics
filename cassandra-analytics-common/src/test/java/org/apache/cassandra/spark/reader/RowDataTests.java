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

import static org.assertj.core.api.Assertions.assertThat;

public class RowDataTests
{
    @Test
    public void testInit()
    {
        RowData rowData = new RowData();
        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNull();
        assertThat(rowData.isNewPartition).isFalse();
    }

    @Test
    public void testSetPartitionKey()
    {
        RowData rowData = new RowData();
        rowData.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        assertThat(rowData.getPartitionKey()).isNotNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNull();

        assertThat(rowData.isNewPartition).isTrue();
        assertThat(rowData.isNewPartition()).isTrue();
        assertThat(rowData.isNewPartition).isFalse();
        assertThat(toString(rowData.getPartitionKey())).isEqualTo("101");

        rowData.setPartitionKeyCopy(ByteBuffer.wrap("102".getBytes()), BigInteger.ZERO);
        assertThat(rowData.isNewPartition).isTrue();
        assertThat(rowData.isNewPartition()).isTrue();
        assertThat(rowData.isNewPartition).isFalse();
        assertThat(toString(rowData.getPartitionKey())).isEqualTo("102");
    }

    @Test
    public void testSetColumnKey()
    {
        RowData rowData = new RowData();
        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNull();
        rowData.setColumnNameCopy(ByteBuffer.wrap("101".getBytes()));

        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNotNull();
        assertThat(rowData.getValue()).isNull();
        assertThat(toString(rowData.getColumnName())).isEqualTo("101");
    }

    @Test
    public void testSetValue()
    {
        RowData rowData = new RowData();
        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNull();
        rowData.setValueCopy(ByteBuffer.wrap("101".getBytes()));

        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNotNull();
        assertThat(toString(rowData.getValue())).isEqualTo("101");
    }

    @Test
    public void testSetAll()
    {
        RowData rowData = new RowData();
        assertThat(rowData.getPartitionKey()).isNull();
        assertThat(rowData.getColumnName()).isNull();
        assertThat(rowData.getValue()).isNull();
        rowData.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        rowData.setColumnNameCopy(ByteBuffer.wrap("102".getBytes()));
        rowData.setValueCopy(ByteBuffer.wrap("103".getBytes()));

        assertThat(rowData.isNewPartition).isTrue();
        assertThat(rowData.isNewPartition()).isTrue();
        assertThat(rowData.isNewPartition).isFalse();

        assertThat(rowData.getPartitionKey()).isNotNull();
        assertThat(rowData.getColumnName()).isNotNull();
        assertThat(rowData.getValue()).isNotNull();
        assertThat(toString(rowData.getPartitionKey())).isEqualTo("101");
        assertThat(toString(rowData.getColumnName())).isEqualTo("102");
        assertThat(toString(rowData.getValue())).isEqualTo("103");

        rowData.setPartitionKeyCopy(ByteBuffer.wrap("104".getBytes()), BigInteger.ZERO);
        assertThat(rowData.isNewPartition).isTrue();
        assertThat(rowData.isNewPartition()).isTrue();
        assertThat(rowData.isNewPartition).isFalse();
        rowData.setColumnNameCopy(ByteBuffer.wrap("105".getBytes()));
        rowData.setValueCopy(ByteBuffer.wrap("106".getBytes()));
        assertThat(toString(rowData.getPartitionKey())).isEqualTo("104");
        assertThat(toString(rowData.getColumnName())).isEqualTo("105");
        assertThat(toString(rowData.getValue())).isEqualTo("106");
    }

    private static String toString(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
