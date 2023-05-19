CREATE KEYSPACE "quoted_keyspace" WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '3', 'datacenter2': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '4', 'datacenter2': '3' } AND DURABLE_WRITES = true;

CREATE TABLE keyspace.tbl1 (
                               id uuid,
                               foo uuid,
                               created_at timestamp,
                               group text,
                               PRIMARY KEY (id)
) WITH gc_grace_seconds = 864000
      AND bloom_filter_fp_chance = 0.1
      AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
      AND comment = ''
      AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
      AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
      AND default_time_to_live = 0
      AND speculative_retry = '99p'
      AND min_index_interval = 128
      AND max_index_interval = 2048
      AND crc_check_chance = 1.0
      AND cdc = false
      AND memtable_flush_period_in_ms = 0;

CREATE INDEX tbl1_foo ON keyspace.tbl1 (foo);

CREATE INDEX tbl1_group_idx ON keyspace.tbl1 (group);

CREATE TABLE keyspace.tbl2 (
                               id uuid,
                               created_at timestamp,
                               hosts text,
                               PRIMARY KEY (id)
) WITH gc_grace_seconds = 864000
      AND bloom_filter_fp_chance = 0.1
      AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
      AND comment = ''
      AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
      AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
      AND default_time_to_live = 0
      AND speculative_retry = '99p'
      AND min_index_interval = 128
      AND max_index_interval = 2048
      AND crc_check_chance = 1.0
      AND cdc = false
      AND memtable_flush_period_in_ms = 0;

CREATE KEYSPACE system_distributed WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system_schema WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE cycling WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '3' } AND DURABLE_WRITES = true;

CREATE TABLE cycling.rank_by_year_and_name (
                                               race_year int,
                                               race_name text,
                                               rank int,
                                               cyclist_name text,
                                               PRIMARY KEY ((race_year, race_name), rank)
) WITH CLUSTERING ORDER BY (rank ASC)
   AND read_repair = 'BLOCKING'
   AND gc_grace_seconds = 864000
   AND additional_write_policy = '99p'
   AND bloom_filter_fp_chance = 0.01
   AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
   AND comment = ''
   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
   AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
   AND default_time_to_live = 0
   AND speculative_retry = '99p'
   AND min_index_interval = 128
   AND max_index_interval = 2048
   AND crc_check_chance = 1.0
   AND cdc = false
   AND memtable_flush_period_in_ms = 0;

CREATE TABLE cycling.rank_by_year_and_name_quoted_columns
(
    race_year      int,
    "RACE_NAME"    text,
    rank           int,
    "cyclist_Name" text,
    PRIMARY KEY ((race_year, "RACE_NAME"), rank)
) WITH CLUSTERING ORDER BY (rank ASC)
   AND read_repair = 'BLOCKING'
   AND gc_grace_seconds = 864000
   AND additional_write_policy = '99p'
   AND bloom_filter_fp_chance = 0.01
   AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
   AND comment = ''
   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
   AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
   AND default_time_to_live = 0
   AND speculative_retry = '99p'
   AND min_index_interval = 128
   AND max_index_interval = 2048
   AND crc_check_chance = 1.0
   AND cdc = false
   AND memtable_flush_period_in_ms = 0;

CREATE INDEX rrank ON cycling.rank_by_year_and_name (rank);

CREATE INDEX ryear ON cycling.rank_by_year_and_name (race_year);

CREATE CUSTOM INDEX rrank_custom ON cycling.rank_by_year_and_name (rank) USING 'org.apache.cassandra.index.CustomIndexTest';
