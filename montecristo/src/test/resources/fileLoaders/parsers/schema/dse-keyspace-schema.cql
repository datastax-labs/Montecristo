CREATE KEYSPACE enterprise_keyspacename WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'BOAW': '3' } AND DURABLE_WRITES = true;

CREATE TABLE enterprise_keyspacename.test (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 ASC)
    AND read_repair_chance = 0.25
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 0
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0
    AND nodesync = { 'enabled' : 'true' };

CREATE KEYSPACE another_enterprise_keyspacename WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'BOAW': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE "OpsCenter" WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '2' } AND DURABLE_WRITES = true;


CREATE TABLE "OpsCenter".pdps (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH CLUSTERING ORDER BY (column1 ASC)
    AND read_repair_chance = 0.25
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 0
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = '{"version": [6, 7, 8], "info": "OpsCenter management data."}'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0
    AND nodesync = { 'enabled' : 'true' };

CREATE KEYSPACE solr_admin WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.EverywhereStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_system WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.EverywhereStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_system_local WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.EverywhereStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system_auth WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'BOAW': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system_traces WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '2' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system_distributed WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE system_schema WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_perf WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_insights WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.EverywhereStrategy' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_security WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'BOAW': '3' } AND DURABLE_WRITES = true;

CREATE KEYSPACE dse_leases WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE KEYSPACE reaper_db WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;
