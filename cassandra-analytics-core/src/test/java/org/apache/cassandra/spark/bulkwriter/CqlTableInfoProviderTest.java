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

package org.apache.cassandra.spark.bulkwriter;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.spark.bulkwriter.CqlTableInfoProvider.removeDeprecatedOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CqlTableInfoProviderTest
{
    @Test
    public void testRemoveDeprecatedOptionsInvalidInput()
    {
        assertThrows(NullPointerException.class, () -> removeDeprecatedOptions(null));
        assertEquals("", removeDeprecatedOptions(""));
        assertEquals("qwerty", removeDeprecatedOptions("qwerty"));
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionNames()
    {
        assertEquals("... WITH qwerty = 42 ...",
                     removeDeprecatedOptions("... WITH qwerty = 42 ..."));
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 42 ..."));
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH dclocal_read_repair_chance = 42 ..."));
        assertEquals("... WITH dclocal_dclocal_read_repair_chance = 42 ...",
                     removeDeprecatedOptions("... WITH dclocal_dclocal_read_repair_chance = 42 ..."));
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionValues()
    {
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = -42 ..."));
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 420.0e-1 ..."));
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = +.42E+1.0 ..."));
        assertEquals("... WITH read_repair_chance = true ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = true ..."));
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionsOrder()
    {
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 1 ..."));
        assertEquals("... WITH qwerty = 42 ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 1 AND qwerty = 42 ..."));
        assertEquals("... WITH qwerty = 42 ...",
                     removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 ..."));
        assertEquals("... WITH qwerty = 42 ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 1 AND qwerty = 42"
                                           + " AND dclocal_read_repair_chance = 1 ..."));
        assertEquals("... WITH qwerty = 42 AND asdfgh = 43 ...",
                     removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 AND asdfgh = 43 ..."));
        assertEquals("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ...",
                     removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 AND asdfgh = 43"
                                           + " AND dclocal_read_repair_chance = 1 AND zxcvbn = 44 ..."));
        assertEquals("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ...",
                     removeDeprecatedOptions("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ..."));
    }

    @Test
    public void testRemoveDeprecatedOptionsStatementCase()
    {
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH read_repair_chance = 1 AND dclocal_read_repair_chance = 1 ..."));
        assertEquals("... WITH ...",
                     removeDeprecatedOptions("... WITH READ_REPAIR_CHANCE = 1 AND DCLOCAL_READ_REPAIR_CHANCE = 1 ..."));
        assertEquals("... with ...",
                     removeDeprecatedOptions("... with read_repair_chance = 1 and dclocal_read_repair_chance = 1 ..."));
        assertEquals("... WiTh ...",
                     removeDeprecatedOptions("... WiTh ReAd_RePaIr_ChAnCe = 1 AnD dClOcAl_ReAd_RePaIr_ChAnCe = 1 ..."));
    }

    @Test
    public void testRemoveDeprecatedOptionsRealStatement()
    {
        String cql = "CREATE TABLE test_simple_rf_3_batch_10000_splits_3.test"
                   + " (id int, course text, foo text, marks int, PRIMARY KEY ((id, course)))"
                   + " WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.0"
                   + " AND gc_grace_seconds = 864000 AND bloom_filter_fp_chance = 0.1"
                   + " AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' } AND comment = ''"
                   + " AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',"
                   +                    " 'enabled' : 'true', 'max_threshold' : 32, 'min_threshold' : 4 }"
                   + " AND compression = { 'chunk_length_in_kb' : 16,"
                   +                     " 'class' : 'org.apache.cassandra.io.compress.ZstdCompressor' }"
                   + " AND default_time_to_live = 0 AND speculative_retry = '99p' AND min_index_interval = 128"
                   + " AND max_index_interval = 2048 AND crc_check_chance = 1.0 AND cdc = false"
                   + " AND memtable_flush_period_in_ms = 0;";
        String expected = "CREATE TABLE test_simple_rf_3_batch_10000_splits_3.test"
                        + " (id int, course text, foo text, marks int, PRIMARY KEY ((id, course)))"
                        + " WITH gc_grace_seconds = 864000 AND bloom_filter_fp_chance = 0.1"
                        + " AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' } AND comment = ''"
                        + " AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',"
                        +                    " 'enabled' : 'true', 'max_threshold' : 32, 'min_threshold' : 4 }"
                        + " AND compression = { 'chunk_length_in_kb' : 16,"
                        +                     " 'class' : 'org.apache.cassandra.io.compress.ZstdCompressor' }"
                        + " AND default_time_to_live = 0 AND speculative_retry = '99p' AND min_index_interval = 128"
                        + " AND max_index_interval = 2048 AND crc_check_chance = 1.0 AND cdc = false"
                        + " AND memtable_flush_period_in_ms = 0;";
        String actual = removeDeprecatedOptions(cql);
        assertEquals(expected, actual);
    }
}
