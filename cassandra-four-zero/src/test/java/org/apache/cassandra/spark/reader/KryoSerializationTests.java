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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KryoSerializationTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    private static final Kryo KRYO = new Kryo();

    static
    {
        BRIDGE.kryoRegister(KRYO);
    }

    @Test
    public void testCdcUpdate()
    {
        ReplicationFactor replicationFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                    ImmutableMap.of("DC1", 3, "DC2", 3));
        BRIDGE.buildSchema("CREATE TABLE cdc.cdc_serialize_test (\n"
                         + "    a uuid PRIMARY KEY,\n"
                         + "    b bigint,\n"
                         + "    c text\n"
                         + ");", "cdc", replicationFactor);
        TableMetadata table = Schema.instance.getTableMetadata("cdc", "cdc_serialize_test");
        long now = System.currentTimeMillis();
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(Clustering.EMPTY);
        row.addCell(BufferCell.live(table.getColumn(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8))),
                                    TimeUnit.MILLISECONDS.toMicros(now),
                                    LongSerializer.instance.serialize(1010101L)));
        row.addCell(BufferCell.live(table.getColumn(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))),
                                    TimeUnit.MILLISECONDS.toMicros(now),
                                    UTF8Serializer.instance.serialize("some message")));
        PartitionUpdate partitionUpdate = PartitionUpdate.singleRowUpdate(table,
                                                                          UUIDSerializer.instance.serialize(UUID.randomUUID()),
                                                                          row.build());
        PartitionUpdateWrapper update = new PartitionUpdateWrapper(table,
                                                                   partitionUpdate,
                                                                   TimeUnit.MILLISECONDS.toMicros(now));
        PartitionUpdateWrapper.Serializer serializer = new PartitionUpdateWrapper.Serializer(table);
        KRYO.register(PartitionUpdateWrapper.class, serializer);

        try (Output out = new Output(1024, -1))
        {
            // Serialize and deserialize the update and verify it matches
            KRYO.writeObject(out, update, serializer);
            PartitionUpdateWrapper deserialized = KRYO.readObject(new Input(out.getBuffer(), 0, out.position()),
                                                                  PartitionUpdateWrapper.class,
                                                                  serializer);
            assertNotNull(deserialized);
            assertEquals(update, deserialized);
            assertEquals(update.digest(), deserialized.digest());
            assertEquals(update.maxTimestampMicros(), deserialized.maxTimestampMicros());
        }
    }
}
