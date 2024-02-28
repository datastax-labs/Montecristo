PRAGMA journal_mode=WAL;

BEGIN IMMEDIATE;


CREATE TABLE IF NOT EXISTS java (
node text,
type text,
key text,
value text,
primary key(node, type, key)
);

CREATE TABLE IF NOT EXISTS tablehistograms (
node text,
keyspace text,
tablename text,
name text,
key text,
value text,
primary key(node, keyspace, tablename, name, key)
);

create index if not exists histo_all_nodes on tablehistograms (keyspace, tablename, name, key);


CREATE TABLE IF NOT EXISTS keyspacehistograms (
node text,
keyspace text,
name text,
key text,
value text,
primary key(node, keyspace, name, key)
);


CREATE TABLE IF NOT EXISTS metrics (
node text,
type text,
scope text,
name text,
key text,
value text,
primary key (node, type, scope, name, key)
);


CREATE TABLE IF NOT EXISTS db (
node text,
keyspace text,
tablename text,
key text,
value text,
primary key(node, keyspace, tablename, key)
);

CREATE TABLE IF NOT EXISTS internal (
node text,
type text,
key text,
value text,
primary key(node, type, key)
);

CREATE TABLE IF NOT EXISTS memory_pool (
node text,
name text,
key text,
value text,
primary key(node, name, key)
);

CREATE TABLE IF NOT EXISTS service (
node text,
name text,
key text,
value text,
PRIMARY KEY (node, name, key)
);

CREATE TABLE IF NOT EXISTS nio (
node text,
type text,
name text,
key text,
value text,
PRIMARY KEY(node, type, name, key)
);

CREATE TABLE IF NOT EXISTS garbage_collector (
node text,
type text,
name text,
pkey text,
key text,
value text,
primary key(node, type, name, pkey, key)

);


CREATE TABLE IF NOT EXISTS failure_detector (
node text,
endpoint text,
phi DOUBLE,
primary key(node, endpoint)
);

CREATE  VIEW IF NOT EXISTS available_table_histograms as select distinct(name) as n from tablehistograms order by n;

CREATE  VIEW IF NOT EXISTS available_metrics as select distinct type, scope from metrics order by 1;

CREATE  VIEW IF NOT EXISTS sstables_per_read_histogram_mean_p99 AS
select t1.node, t1.keyspace, t1.tablename, CAST(t1.value as int) as mean, CAST(t2.value as int) as p99
FROM tablehistograms t1
LEFT JOIN tablehistograms t2 ON t1.node = t2.node AND t1.keyspace = t2.keyspace AND t1.tablename = t2.tablename
WHERE t1.name = 'SSTablesPerReadHistogram' AND t2.name = 'SSTablesPerReadHistogram'
AND mean > 0
AND t1.keyspace != ""
AND t1.key = 'Mean' AND t2.key = '99thPercentile';

CREATE  VIEW IF NOT EXISTS sstables_per_read_histogram_p75 AS
select keyspace, tablename, name, key, max(CAST(value AS DECIMAL(15,2))) from tablehistograms
WHERE name in ('SSTablesPerReadHistogram') and key in ('75thPercentile')
group by 1,2,3,4
order by 5
desc;

CREATE  VIEW IF NOT EXISTS bloom_filters as
SELECT t1.node, t1.keyspace, t1.tablename,
round(t1.value, 5) as "FalsePositiveRatio",
t2.value / 1024 / 1024 as "OffHeapMemoryUsedInMB"
FROM
(select * from tablehistograms
where name = 'BloomFilterFalseRatio') t1
JOIN
(select * from tablehistograms
where name = 'BloomFilterOffHeapMemoryUsed') t2
ON t1.node = t2.node AND t1.keyspace = t2.keyspace and t1.tablename = t2.tablename;


CREATE  VIEW IF NOT EXISTS blocked_threadpools as
select * from metrics
WHERE type = 'ThreadPools' and name like '%Blocked%' and key = 'Count' and CAST(value AS INT) > 0 ;

CREATE  VIEW IF NOT EXISTS reclaimable_space_from_tombstones as
select keyspace, tablename, sum(reclaimable) from (
select a.node, a.keyspace, a.tablename, b.value as droppable_ratio, a.value as live_size, CAST(a.value as double)*CAST(b.value AS double) as reclaimable
from tablehistograms a, db b
where b.key = 'DroppableTombstoneRatio'
and a.name = 'LiveDiskSpaceUsed'
and a.keyspace = b.keyspace
and a.tablename = b.tablename
and a.node = b.node
and CAST(b.value as double) > 0.2)
group by 1,2
order by 3 desc;

CREATE  VIEW IF NOT EXISTS max_partition_sizes as
select a.keyspace, a.tablename, max(CAST(a.value AS DECIMAL(15,2))) as max_partition_size
from tablehistograms a
where a.name = 'MaxRowSize'
and keyspace != ''
AND CAST(a.value AS DECIMAL(15,2)) > 100000000
group by 1, 2
order by 3 desc;

CREATE  VIEW IF NOT EXISTS live_disk_space_used as
select a.keyspace, a.node, sum(a.value) as live_size
from tablehistograms a
where a.name = 'LiveDiskSpaceUsed'
group by 1, 2
order by 3 desc;

CREATE  VIEW IF NOT EXISTS droppable_tombstone_ratio as
select a.keyspace, a.tablename, min(CAST(b.value AS DECIMAL(15,2))) as droppable_ratio
from tablehistograms a, db b
where b.key = 'DroppableTombstoneRatio'
and a.name = 'LiveDiskSpaceUsed'
and a.keyspace = b.keyspace
and a.tablename = b.tablename
and a.node = b.node
and a.keyspace not like 'system%'
and CAST(b.value as double) > 0.1
group by 1,2
order by 3 desc, 1,2;

CREATE  VIEW IF NOT EXISTS tombstones_scanned as
select a.keyspace, a.tablename, avg(CAST(value as double)) as p999
from tablehistograms a where name = 'TombstoneScannedHistogram' and key='95thPercentile' and keyspace != '' and keyspace NOT LIKE 'system%' and tablename != ''
and CAST(value as double) > 0
group by 1,2
order by 3 desc
limit 10;

CREATE  VIEW IF NOT EXISTS list_compaction_strategies as
SELECT distinct keyspace, tablename, key, value from db where key = 'CompactionParametersJson' and keyspace not like 'system%';

CREATE  VIEW IF NOT EXISTS top_tables_by_write_latencies as
select p75.keyspace, p75.tablename, ROUND(max(p50.read_latency),2) as p50, ROUND(avg(CAST(value as double)),2) as p75, ROUND(max(p99.read_latency),2) as p99
from tablehistograms p75 ,
(select a.keyspace, a.tablename, avg(CAST(value as double)) as read_latency
from tablehistograms a
where name = 'WriteLatency' and key='50thPercentile' and keyspace != ''
and keyspace NOT LIKE 'system%' and tablename != ''
group by 1,2) as p50,
(select a.keyspace, a.tablename, avg(CAST(value as double)) as read_latency
from tablehistograms a
where name = 'WriteLatency' and key='99thPercentile' and keyspace != ''
and keyspace NOT LIKE 'system%' and tablename != ''
group by 1,2) as p99
where p75.name = 'WriteLatency' and p75.key='75thPercentile' and p75.keyspace != ''
and p75.keyspace NOT LIKE 'system%' and p75.tablename != ''
and p75.keyspace = p50.keyspace
and p75.tablename = p50.tablename
and p75.keyspace = p99.keyspace
and p75.tablename = p99.tablename
group by 1,2
order by 5 desc
limit 10;

CREATE  VIEW IF NOT EXISTS top_tables_by_read_latencies as
select p75.keyspace, p75.tablename, ROUND(max(CAST(value as double)),2) as p75, ROUND(max(p99.read_latency),2) as p99, readCount.reads, p75SStables.sstables as p75_sstables
from tablehistograms p75 ,
(select a.keyspace, a.tablename, max(CAST(value as double)) as read_latency
from tablehistograms a
where name = 'ReadLatency' and key='99thPercentile' and keyspace != ''
and keyspace NOT LIKE 'system%' and tablename != ''
group by 1,2) as p99,
(select a.keyspace, a.tablename, max(CAST(value as double)) as sstables
        from tablehistograms a
        where name = 'SSTablesPerReadHistogram' and key='75thPercentile' and keyspace != ''
        and keyspace NOT LIKE 'system%' and tablename != ''
        and value > 0
        group by 1,2) as p75SStables,
(SELECT keyspace, tablename, sum(CAST(value as double)) as reads FROM tablehistograms WHERE name = 'ReadLatency' AND key='Count' and keyspace != '' AND tablename != '' GROUP BY 1,2 ) as readCount
where p75.name = 'ReadLatency' and p75.key='75thPercentile' and p75.keyspace != ''
and p75.keyspace NOT LIKE 'system%' and p75.tablename != ''
and p75.keyspace = p99.keyspace
and p75.tablename = p99.tablename
and p75.keyspace = readCount.keyspace
and p75.tablename = readCount.tablename
and p75.keyspace = p75SStables.keyspace
and p75.tablename = p75SStables.tablename
group by 1,2
order by 4 desc
limit 20;

CREATE  VIEW IF NOT EXISTS top_tables_by_sstables_per_read as
select p75.keyspace, p75.tablename, ROUND(max(p75.read_latency),2) as p75, ROUND(max(CAST(value as double)),2) as p95, ROUND(max(p99.read_latency),2) as p99
from tablehistograms p95 ,
        (select a.keyspace, a.tablename, max(CAST(value as double)) as read_latency
        from tablehistograms a
        where name = 'SSTablesPerReadHistogram' and key='75thPercentile' and keyspace != ''
        and keyspace NOT LIKE 'system%' and tablename != ''
        and value > 0
        group by 1,2) as p75,
        (select a.keyspace, a.tablename, max(CAST(value as double)) as read_latency
        from tablehistograms a
        where name = 'SSTablesPerReadHistogram' and key='99thPercentile' and keyspace != ''
        and keyspace NOT LIKE 'system%' and tablename != ''
        and value > 0
        group by 1,2) as p99
        where p95.name = 'SSTablesPerReadHistogram' and p95.key='95thPercentile' and p95.keyspace != ''
        and p95.value > 0
        and p95.keyspace NOT LIKE 'system%' and p75.tablename != ''
        and p95.keyspace = p75.keyspace
        and p95.tablename = p75.tablename
        and p95.keyspace = p99.keyspace
        and p95.tablename = p99.tablename
        group by 1,2
        order by 3 desc
        limit 20;

CREATE  VIEW IF NOT EXISTS top_tables_by_p99_read_latency as
SELECT a.keyspace, a.tablename, max(ops.reads) as reads, max(ops.writes) as writes, max(ops.reads + ops.writes) as ops, max(ops.read_write_ratio) as read_write_ratio, max(read_latency.p99_read_latency) as p99_read_latency, max(write_latency.p99_write_latency) as p99_write_latency, max(sstables_per_read.p75) as p75_sstables_per_read, max(sstables_per_read.p99) as p99_sstables_per_read, max(live_scanned.avg_live_scan) as avg_live_scan, max(live_scanned.max_live_scan) as max_live_scan, max(tombstone_scanned.avg_tombstone_scan) as avg_tombstone_scan, max(tombstone_scanned.max_tombstone_scan) as max_tombstone_scan, max(CAST(a.value AS DECIMAL(15,2))) as max_partition_size
FROM tablehistograms a LEFT OUTER JOIN
        (select p75.keyspace, p75.tablename, ROUND(avg(p75.read_latency),2) as p75, ROUND(avg(CAST(value as double)),2) as p95, ROUND(avg(p99.read_latency),2) as p99
        from tablehistograms p95 ,
        (select a.keyspace, a.tablename, avg(CAST(value as double)) as read_latency
        from tablehistograms a
        where name = 'SSTablesPerReadHistogram' and key='75thPercentile' and keyspace != ''
        and keyspace NOT LIKE 'system%' and tablename != ''
        and CAST(value AS INT) > 0
        group by 1,2) as p75,
        (select a.keyspace, a.tablename, avg(CAST(value as double)) as read_latency
        from tablehistograms a
        where name = 'SSTablesPerReadHistogram' and key='99thPercentile' and keyspace != ''
        and keyspace NOT LIKE 'system%' and tablename != ''
        and CAST(value AS INT) > 0
        group by 1,2) as p99
        where p95.name = 'SSTablesPerReadHistogram' and p95.key='95thPercentile' and p95.keyspace != ''
        and CAST(p95.value AS INT) > 0
        and p95.keyspace NOT LIKE 'system%' and p75.tablename != ''
        and p95.keyspace = p75.keyspace
        and p95.tablename = p75.tablename
        and p95.keyspace = p99.keyspace
        and p95.tablename = p99.tablename
        group by 1,2) as sstables_per_read on a.keyspace = sstables_per_read.keyspace and a.tablename = sstables_per_read.tablename,

        (SELECT a.keyspace, a.tablename, sum(CAST(value as double)) as reads, max(b.writes) as writes, sum(CAST(value as double))/max(b.writes) as read_write_ratio
        FROM tablehistograms a,
        (SELECT keyspace, tablename, sum(CAST(value as double)) as writes FROM tablehistograms WHERE name = 'WriteLatency' AND key='Count' and keyspace != '' AND tablename != '' GROUP BY 1,2 ) as b
        WHERE a.name = 'ReadLatency'
          AND a.key='Count' and a.keyspace != ''
          AND a.tablename != ''
          AND a.keyspace = b.keyspace
          AND a.tablename = b.tablename
        GROUP BY 1,2) as ops,

        (select keyspace, tablename, ROUND(avg(CAST(value as double)),2) as p99_write_latency
        from tablehistograms p99
        where p99.name = 'WriteLatency' and p99.key='99thPercentile' and p99.keyspace != ''
        and p99.keyspace NOT LIKE 'system%' and p99.tablename != ''
        group by 1,2) as write_latency,

        (select keyspace, tablename, ROUND(avg(CAST(value as double)),2) as p99_read_latency
        from tablehistograms p99
        where p99.name = 'ReadLatency' and p99.key='99thPercentile' and p99.keyspace != ''
        and p99.keyspace NOT LIKE 'system%' and p99.tablename != ''
        group by 1,2) as read_latency,

        (select a.keyspace, a.tablename, avg(CAST(value as double)) as avg_live_scan, max(CAST(value as double)) as max_live_scan
        from tablehistograms a where name = 'LiveScannedHistogram' and key='99thPercentile' and keyspace != '' and keyspace NOT LIKE 'system%' and tablename != ''
        and CAST(value as double) > 0
        group by 1,2) as live_scanned,

        (select a.keyspace, a.tablename, avg(CAST(value as double)) as avg_tombstone_scan, max(CAST(value as double)) as max_tombstone_scan
        from tablehistograms a where name = 'TombstoneScannedHistogram' and key='99thPercentile' and keyspace != '' and keyspace NOT LIKE 'system%' and tablename != ''
        and CAST(value as double) > 0
        group by 1,2) as tombstone_scanned

    where a.name = 'MaxRowSize'
    and a.keyspace != ''
    and a.keyspace = ops.keyspace
    and a.keyspace = write_latency.keyspace
    and a.keyspace = read_latency.keyspace
    and a.keyspace = live_scanned.keyspace
    and a.keyspace = tombstone_scanned.keyspace
    and a.tablename = ops.tablename
    and a.tablename = write_latency.tablename
    and a.tablename = read_latency.tablename
    and a.tablename = live_scanned.tablename
    and a.tablename = tombstone_scanned.tablename
    group by 1, 2
    order by p99_read_latency desc;

CREATE TABLE IF NOT EXISTS servers (
  host text primary key,
  memory_in_gb int,
  aws_region text,
  aws_instance_type text,
  has_jmx boolean
);



COMMIT;

