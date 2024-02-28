## Compaction settings


Compaction is used by the Log-structured Merge Tree storage engine in {{software}} to reclaim wasted disk and improve the performance of the read path. It is a key component of
{{software}} performance tuning, and is controlled by the compaction table property.  

The rate at which data can be compacted is controlled by `compaction_throughput_mb_per_sec` and the number of simultaneous compactors is controlled by `concurrent_compactors`.  
See the {{software}}-Configuration/Compaction section above.

There is a useful setting for logging when partitions get to be over a certain size, `compaction_large_partition_warning_threshold_mb`, which defaults to 100MB.


### Compaction Strategies


There are currently four compaction strategies available in {{software}}:

* **SizeTieredCompactionStrategy** (STCS) the default, good for data that is written to for a short time and then mostly read from. It groups SSTables together based on their size.  
* **DateTieredCompactionStrategy** (DTCS) is used for data that is written in time series order. It groups data together by timestamp, and stops compacting older data. This strategy is deprecated and should not be used anymore.
* **LeveledCompactionStrategy** (LCS) is used for low latency read or high overwrite and TTL workloads. It groups SSTables into levels of files that are the same size.  
* **TimeWindowCompactionStrategy** (TWCS) is similar to DTCS and is designed for time series ordered data. It groups data together by maximum timestamp, and stops compacting older data. This strategy has replaced DTCS and was integrated into Cassandra 3.0.8 / 3.8. It can be used with other versions of Cassandra by adding the appropriate jar in the _cassandra/lib_ directory

It's also possible to use custom compaction strategies, such as the DeletingCompactionStrategy.

The compaction strategies used in this cluster (k = thousand, M = million, B = billion, T = Trillion):

{{tables}}

{{#toSTCS}}

We recommend evaluating the following tables to use `SizeTieredCompactionStrategy`:

{{toSTCS}}

{{/toSTCS}}

{{#toLCS}}

We recommend evaluating the following tables to use `LeveledCompactionStrategy`:

{{toLCS}}

{{/toLCS}}

{{#toTWCS}}

We recommend evaluating the opportunity for the following tables to use `TimeWindowCompactionStrategy` as they have been identified as potential time series or are currently using the deprecated `DateTieredCompactionStrategy`:

{{toTWCS}}

{{/toTWCS}}

### Pending Compactions

Pending compactions are a calculation to indicate how many compaction tasks are required to reach a "perfect" state.  Each compaction strategy has different configuration options which affect how many pending tasks are reported.

This number should be as close to zero as possible.  

{{^tablesWithPendingMd}}
There are no tables with pending compactions.
{{/tablesWithPendingMd}}

{{#tablesWithPendingMd}}

The following tables have pending compactions:

{{tablesWithPendingMd}}

{{/tablesWithPendingMd}}


### TimeWindowCompactionStrategy

There are operational requirements to the use of TimeWindowCompactionStrategy (TWCS) and tuning required for optimal expiry of TTL'd data.

#### TWCS Operational Requirements

TimeWindowCompactionStrategy (TWCS) can only be used when rows in each partition are chronological sorted (ascending or descending), and writes to the system occur in roughly* the same chronologically order (ignoring any difference between ascending or descending). Updates or deletes to existing data and out-of-order row inserts to existing partitions, more than one time_window apart, cannot happen.

The TWCS table is configured with a “time window” with the `compaction_window_unit` and `compaction_window_size` settings. Data resides in different time windows based on when the write arrives into Cassandra. While is it ok (and typical) that partitions span over multiple windows, the ordering of rows within partitions should not be out of order between windows.

*roughly) The order of writes arriving in Cassandra does not need to be exact. It is ok for writes to arrive at any time in the correct window. It is also expected that there will be some overlap/mix between adjacent time windows. For example when a window rolls over there will also be some writes in-flight that land into the next window.

What is important with the order of writes is to avoid writes landing in non-adjacent time windows. If writes can be buffered over longer periods of time, e.g. offline IoT devices, then the time window should be made big enough so the write never comes in later than the next time window. This problem can also occur with read repairs. Before Cassandra version 4.0 both `*read_repair_chance` settings on the table set to `0.0`). And with Cassandra version 4.0 and greater TWCS tables should have `read_repair` set to `NONE` so also disable the synchronous read repairs.

#### How TWCS Data Expires

When data goes past it’s time-to-live (TTL) value it is considered tombstoned, and is not readable from the client. Tombstoned data is kept for a `gc_grace_seconds` amount of time. After `gc_grace_seconds` has passed then it is considered expired. Other compaction strategies will remove this expired data once it becomes included in a compaction.

TWCS removes expired data from the disk in a different manner. There are two different mechanisms involved.
 - Single SSTable Tombstone Compaction
 - Fully Expired SSTables

##### Single SSTable Tombstone Compaction

The data in the oldest time window SSTable will slowly expire over the course of its time window. As the ratio of expired to live data increases, a [tombstone compaction](https://cassandra.apache.org/doc/3.11/cassandra/operating/compaction/index.html#single-sstable-tombstone-compaction) can initiate. As the name implies, a tombstone compaction operates only on a single SSTable at a time. Tombstone compactions only happen when there are no other pending compactions, and only happen at `tombstone_compaction_interval` frequency. And then tombstone compactions will start only on individual SSTables that have an expired to live data ratio higher than `tombstone_threshold`. Furthermore, as an optimisation there is check in place to prevent tombstone compactions that may end up not deleting any data. The check looks to see if any partitions in the SSTable overlap with the same partition in other SSTables. These checks can be disabled with `unchecked_tombstone_compaction`, this will not alter what data ends up being deleted, it only makes the running of tombstone compactions eager.

##### Fully Expired SSTables

If an SSTable only contains expired data then the whole SSTable file can simply be [deleted off disk](https://cassandra.apache.org/doc/3.11/cassandra/operating/compaction/index.html#fully-expired-sstables). This is an almost instantaneous action avoiding the whole compaction process.

**If all data in a TWCS table is written with the same TTL then there are significant gains in tuning the table to drop expired SSTables and not do tombstone compactions.**

Expired SSTables will not be deleted if SSTables from newer time windows have older overlapping partition data. This is important to protect against situations where data is updated, but the update arrived on the node before the original data. In such a situation deleting the expired SSTable with the update data would leave only the original data on the node, breaking consistency with stale/old data being returned to the client. If you are confident this situation does not apply to you it is possible to disable this restriction by setting `unsafe_aggressive_sstable_expiration` to `true`.  Due to the risk of this option, the node’s jvm must also be started (add to `jvm*.options` conf file) with the flag  `-Dcassandra.unsafe_aggressive_sstable_expiration=true`.

#### How to set gc_grace_seconds on TWCS Tables Effectively

Because TTL data interacts with `gc_grace_seconds` in a different manner, tables that only contain TTL’d data (i.e. no manual deletes) for example tables using TWCS, then `gc_grace_seconds` should be set to be (reasonably) larger than the smaller of `max_hint_window_ms` and data TTL.

#### More Information…

More information on TWCS and tombstones can be read in the following…
 - [About Deletes and Tombstones in Cassandra](https://thelastpickle.com/blog/2016/07/27/about-deletes-and-tombstones.html)
 - [How to Choose which Tombstones to Drop](https://thelastpickle.com/blog/2019/09/11/how-to-choose-which-tombstones-to-drop.html)
 - [TWCS part 1 - how does it work and when should you use it ?](https://thelastpickle.com/blog/2016/12/08/TWCS-part1.html)
 - [Compaction | Apache Cassandra Documentation](https://cassandra.apache.org/doc/3.11/cassandra/operating/compaction/index.html)
 - [Hinted Handoff and GC Grace Demystified](https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html)


<!---
 TODO – Report/Recommendations to implement

 # Set read_repair to NONE on all TWCS Tables (if using Cassandra version >= 4.0)
 # Set tombstone_compaction_interval to match the time window size, on all TWCS Tables
 # Report Per TWCS Table: time_window size, tombstone_compaction_interval, number of time_windows on disk, tombstone ratio
 # Report Per TWCS Table: some summary of the increase of time_window's tombstone ratios
--->
