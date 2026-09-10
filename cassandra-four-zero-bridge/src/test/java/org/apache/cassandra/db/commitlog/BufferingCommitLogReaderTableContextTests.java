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

package org.apache.cassandra.db.commitlog;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferingCommitLogReaderTableContextTests
{
    @Test
    public void testFailedMutationHasNoCdcTableWhenNonCdcTableIdReadableButRestCorrupted() throws Exception
    {
        TableId tableId = registerTable("ks_non_cdc", "tbl", false);
        byte[] corruptedMutation = buildCorruptedMutation(1, tableId);

        boolean failedMutationHasNoCdcTable = BufferingCommitLogReader.failedMutationHasNoCdcTable(corruptedMutation,
                                                                                                     corruptedMutation.length);

        assertThat(failedMutationHasNoCdcTable).isTrue();
    }

    @Test
    public void testFailedMutationHasNoCdcTableWhenCdcTableIdReadableButRestCorrupted() throws Exception
    {
        TableId tableId = registerTable("ks_cdc", "tbl", true);
        byte[] corruptedMutation = buildCorruptedMutation(1, tableId);

        boolean failedMutationHasNoCdcTable = BufferingCommitLogReader.failedMutationHasNoCdcTable(corruptedMutation,
                                                                                                     corruptedMutation.length);

        assertThat(failedMutationHasNoCdcTable).isFalse();
    }

    @Test
    public void testFailedMutationHasNoCdcTableWhenTableIdUnresolvable() throws Exception
    {
        CassandraBridgeImplementation.setup();
        TableId unknownTableId = TableId.fromUUID(UUID.randomUUID());
        byte[] corruptedMutation = buildCorruptedMutation(1, unknownTableId);

        boolean failedMutationHasNoCdcTable = BufferingCommitLogReader.failedMutationHasNoCdcTable(corruptedMutation,
                                                                                                     corruptedMutation.length);

        // an unresolvable table is treated as not CDC-enabled
        assertThat(failedMutationHasNoCdcTable).isTrue();
    }

    @Test
    public void testFailedMutationHasNoCdcTableWhenMultipleUpdatesEvenIfFirstIsNonCdc() throws Exception
    {
        TableId tableId = registerTable("ks_non_cdc_multi", "tbl", false);
        byte[] corruptedMutation = buildCorruptedMutation(2, tableId);

        boolean failedMutationHasNoCdcTable = BufferingCommitLogReader.failedMutationHasNoCdcTable(corruptedMutation,
                                                                                                     corruptedMutation.length);

        // can't safely check the remaining update(s) without deserializing this one's body, so this is
        // conservatively treated as possibly involving a CDC-enabled table
        assertThat(failedMutationHasNoCdcTable).isFalse();
    }

    private TableId registerTable(String keyspaceName, String tableName, boolean cdc)
    {
        CassandraBridgeImplementation.setup();
        KeyspaceMetadata keyspaceMetadata = KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(1));
        Schema.instance.load(keyspaceMetadata);
        Keyspace.openWithoutSSTables(keyspaceName);

        String createTableStatement = "CREATE TABLE " + keyspaceName + "." + tableName + " (pk int PRIMARY KEY)"
                                     + (cdc ? " WITH cdc = true" : "");
        TableMetadata tableMetadata = CQLFragmentParser
                                      .parseAny(CqlParser::createTableStatement, createTableStatement, "CREATE TABLE")
                                      .keyspace(keyspaceName)
                                      .prepare(null)
                                      .builder(Types.none())
                                      .build();
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        Schema.instance.load(keyspace.withSwapped(keyspace.tables.with(tableMetadata)));
        return tableMetadata.id;
    }

    // Builds a mutation buffer by hand: a valid header (vint update count, followed by the real TableId of the
    // first update) followed by garbage bytes standing in for a partition update body that fails to deserialize
    // (e.g. due to a schema mismatch). The checksum in BufferingCommitLogReader already guarantees these bytes
    // aren't randomly corrupted, so this reproduces a mutation whose first TableId is intact but whose body
    // cannot be parsed.
    private byte[] buildCorruptedMutation(int updateCount, TableId firstTableId) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeUnsignedVInt(updateCount);
            firstTableId.serialize(out);
            out.write(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
            return out.toByteArray();
        }
    }
}
