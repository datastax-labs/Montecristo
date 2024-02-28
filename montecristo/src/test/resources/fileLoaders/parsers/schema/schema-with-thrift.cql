CREATE KEYSPACE "Thrift" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}  AND durable_writes = true;

CREATE TABLE "Thrift"."ExampleTable1" (
    key blob,
    column1 blob,
    column2 blob,
    "" map<blob, blob>,
    value blob,
    PRIMARY KEY (key, column1, column2)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC, column2 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '4', 'class': 'org.apache.cassandra.io.compress.SnappyCompressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 172800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.1
    AND speculative_retry = 'NONE';