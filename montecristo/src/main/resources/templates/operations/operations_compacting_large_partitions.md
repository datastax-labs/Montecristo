## Compacting Large Partitions

For {{software}} it’s recommended to keep the size of partitions under 100MB. For Apache Cassandra from version 3.11 the recommendation is raised to keep partitions under 1GB. Usually the presences of the large partitions is a sign of incorrect data model, triggered by one or more of these factors:

- Low cardinality of partition keys. That is, when too few possible values exist for partition keys.
- Non-uniform spread of data between partitions.
For example, consider the case where a customer ID is used as a partition key. If the size of the data collected for some customers is significantly larger than the others, then some partition keys will be bigger than others. As a result, some nodes may have much more data than other nodes. More data increases the load on these nodes because they handle more requests, require more compaction, and so on.
- Too many columns and rows in the table, especially when every row contains data for all or most columns.
- Storing big blobs or long texts in the table.
- Large partitions create an additional load on {{software}}, such as allocation of additional memory to hold partition index.  
**Note:** Before Cassandra versions 3.6, reading large partitions put much more pressure on Java heap and often led to nodes crashing.
- Uneven data distribution between the nodes can lead to hotspots when some nodes handle many more requests than others.
- Large partitions require transferring more data when performing reading of the whole partition.
- {{software}} partition size can affect external systems, such as Spark, because {{software}}’s partition is the minimal object mapped into the Spark partition; any imbalance in {{software}} may lead to imbalance when processing data with Spark.  
Compactions exceeding this limit trigger a log message that looks similar to:

```
WARN  [CompactionExecutor:787] 2017-11-27 15:46:46,132 SSTableWriter.java:240 - Compacting large partition keyspace/table:<primary_key> (148802470 bytes)
```
{{#largePartitionsWarningsByTable}}
Warnings by Table:

{{largePartitionsWarningsByTable}}

Warnings by Date:

{{largePartitionsWarningsByDate}}

Largest Warning per Table:

{{largestPartitionsPerTable}}
{{/largePartitionsWarningsByTable}}
{{^largePartitionsWarningsByTable}}
No large partition warnings have been found within the logs.
{{/largePartitionsWarningsByTable}}

{{software}} tracks statistics on partition sizes internally in addition to logs.

{{largePartitionsTableFromMetrics}}
