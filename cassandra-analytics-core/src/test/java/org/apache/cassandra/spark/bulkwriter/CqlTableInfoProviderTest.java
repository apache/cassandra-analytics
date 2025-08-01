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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CqlTableInfoProviderTest
{
    @Test
    public void testRemoveDeprecatedOptionsInvalidInput()
    {
        assertThatThrownBy(() -> removeDeprecatedOptions(null)).isInstanceOf(NullPointerException.class);
        assertThat(removeDeprecatedOptions("")).isEqualTo("");
        assertThat(removeDeprecatedOptions("qwerty")).isEqualTo("qwerty");
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionNames()
    {
        assertThat(removeDeprecatedOptions("... WITH qwerty = 42 ...")).isEqualTo("... WITH qwerty = 42 ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 42 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH dclocal_read_repair_chance = 42 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH dclocal_dclocal_read_repair_chance = 42 ...")).isEqualTo("... WITH dclocal_dclocal_read_repair_chance = 42 ...");
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionValues()
    {
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = -42 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 420.0e-1 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = +.42E+1.0 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = true ...")).isEqualTo("... WITH read_repair_chance = true ...");
    }

    @Test
    public void testRemoveDeprecatedOptionsOptionsOrder()
    {
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 1 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 1 AND qwerty = 42 ...")).isEqualTo("... WITH qwerty = 42 ...");
        assertThat(removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 ...")).isEqualTo("... WITH qwerty = 42 ...");
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 1 AND qwerty = 42"
                                           + " AND dclocal_read_repair_chance = 1 ...")).isEqualTo("... WITH qwerty = 42 ...");
        assertThat(removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 AND asdfgh = 43 ...")).isEqualTo("... WITH qwerty = 42 AND asdfgh = 43 ...");
        assertThat(removeDeprecatedOptions("... WITH qwerty = 42 AND read_repair_chance = 1 AND asdfgh = 43"
                                           + " AND dclocal_read_repair_chance = 1 AND zxcvbn = 44 ...")).isEqualTo("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ...");
        assertThat(removeDeprecatedOptions("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ...")).isEqualTo("... WITH qwerty = 42 AND asdfgh = 43 AND zxcvbn = 44 ...");
    }

    @Test
    public void testRemoveDeprecatedOptionsStatementCase()
    {
        assertThat(removeDeprecatedOptions("... WITH read_repair_chance = 1 AND dclocal_read_repair_chance = 1 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... WITH READ_REPAIR_CHANCE = 1 AND DCLOCAL_READ_REPAIR_CHANCE = 1 ...")).isEqualTo("... WITH ...");
        assertThat(removeDeprecatedOptions("... with read_repair_chance = 1 and dclocal_read_repair_chance = 1 ...")).isEqualTo("... with ...");
        assertThat(removeDeprecatedOptions("... WiTh ReAd_RePaIr_ChAnCe = 1 AnD dClOcAl_ReAd_RePaIr_ChAnCe = 1 ...")).isEqualTo("... WiTh ...");
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
        assertThat(actual).isEqualTo(expected);
    }
}
