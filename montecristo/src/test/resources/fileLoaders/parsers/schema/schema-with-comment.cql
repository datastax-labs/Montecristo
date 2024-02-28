/* VIRTUAL KEYSPACE system_virtual_schema WITH REPLICATION = { 'class' : 'null' } AND DURABLE_WRITES = false;*/

CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3'}  AND durable_writes = true;

CREATE TABLE test.basetable (
    id timeuuid PRIMARY KEY,
    itemid text,
    createdate timestamp,
    createdby text,
    interaction timeuuid,
    lastmodifiedate timestamp
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

/* VIRTUAL KEYSPACE system_views WITH REPLICATION = { 'class' : 'null' } AND DURABLE_WRITES = false;*/

/* VIRTUAL TABLE system_views.caches (
    name text,
    capacity_bytes bigint,
    entry_count int,
    hit_count bigint,
    hit_ratio double,
    recent_hit_rate_per_second bigint,
    recent_request_rate_per_second bigint,
    request_count bigint,
    size_bytes bigint,
    PRIMARY KEY (name)
) ; */