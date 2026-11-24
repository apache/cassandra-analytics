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

package org.apache.cassandra.cdc.msg;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the CDC events built from mutations of a non-frozen UDT column {@code result (status text, code int)}
 */
public class FourZeroCdcEventBuilderTest
{
    private static final long TIMESTAMP = 1_000_000L;
    private static final int NOW_IN_SEC = 1;

    private static final UserType UDT = new UserType("ks", UTF8Type.instance.decompose("single_result"),
                                                     Arrays.asList(FieldIdentifier.forUnquoted("status"),
                                                                   FieldIdentifier.forUnquoted("code")),
                                                     Arrays.<AbstractType<?>>asList(UTF8Type.instance, Int32Type.instance),
                                                     true);
    private static final TableMetadata TABLE = TableMetadata.builder("ks", "tbl")
                                                            .partitioner(Murmur3Partitioner.instance)
                                                            .addPartitionKeyColumn("id", Int32Type.instance)
                                                            .addRegularColumn("result", UDT)
                                                            .build();
    private static final ColumnMetadata RESULT = TABLE.getColumn(ByteBufferUtil.bytes("result"));

    @Test
    public void testPartialUpdate()
    {
        // UPDATE tbl SET result.code = 500 WHERE id = 1
        Row.Builder row = newRow();
        row.addCell(BufferCell.live(RESULT, TIMESTAMP, Int32Type.instance.decompose(500), field(1)));

        CdcEvent event = build(row.build());

        assertThat(resultValue(event)).isEqualTo(packed(null, Int32Type.instance.decompose(500)));
        assertThat(event.getUpdatedUdtFields()).isEqualTo(Collections.singletonMap("result", Collections.singletonList("code")));
    }

    @Test
    public void testFieldSetToNull()
    {
        // UPDATE tbl SET result.status = null WHERE id = 1
        Row.Builder row = newRow();
        row.addCell(BufferCell.tombstone(RESULT, TIMESTAMP, NOW_IN_SEC, field(0)));

        CdcEvent event = build(row.build());

        assertThat(event.getKind()).isEqualTo(CdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
        assertThat(resultValue(event)).isEqualTo(packed(null, null));
        assertThat(event.getUpdatedUdtFields()).isEqualTo(Collections.singletonMap("result", Collections.singletonList("status")));
    }

    @Test
    public void testFullWriteWithMissingField()
    {
        // INSERT INTO tbl (id, result) VALUES (1, {status: 'OK'}) overwrites the UDT, so code is null
        Row.Builder row = newRow();
        row.addComplexDeletion(RESULT, new DeletionTime(TIMESTAMP - 1, NOW_IN_SEC));
        row.addCell(BufferCell.live(RESULT, TIMESTAMP, UTF8Type.instance.decompose("OK"), field(0)));

        CdcEvent event = build(row.build());

        assertThat(resultValue(event)).isEqualTo(packed(UTF8Type.instance.decompose("OK"), null));
        assertThat(event.getUpdatedUdtFields()).isNull();
    }

    private static Row.Builder newRow()
    {
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(Clustering.EMPTY);
        return row;
    }

    private static CellPath field(int position)
    {
        return CellPath.create(ByteBufferUtil.bytes((short) position));
    }

    private static CdcEvent build(Row row)
    {
        DecoratedKey key = TABLE.partitioner.decorateKey(Int32Type.instance.decompose(1));
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(TABLE, key, row);
        return FourZeroCdcEventBuilder.build(CdcEvent.Kind.INSERT, update.unfilteredIterator(), row, null, CassandraSource.DEFAULT);
    }

    private static ByteBuffer resultValue(CdcEvent event)
    {
        List<Value> values = event.getValueColumns();
        assertThat(values).hasSize(1);
        return values.get(0).getValue();
    }

    // non-frozen UDT encoding: [int field count] followed by [int length][bytes] per field, length -1 for null
    private static ByteBuffer packed(ByteBuffer status, ByteBuffer code)
    {
        ByteBuffer result = ByteBuffer.allocate(64);
        result.putInt(2);
        for (ByteBuffer field : new ByteBuffer[]{status, code})
        {
            if (field == null)
            {
                result.putInt(-1);
            }
            else
            {
                result.putInt(field.remaining());
                result.put(field.duplicate());
            }
        }
        result.flip();
        return result;
    }
}
