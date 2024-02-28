## Repairs

{{software}} is designed to remain operational, even when faults and outages occur within a cluster. When faults do occur, the data between the nodes can become out of sync. The nature of the inconsistency will be based on the consistency level requested during a write operation that has encountered the outage. This is sometimes referred to as 'entropy' within the cluster.

The job of repair is to remove this entropy and bring the cluster to a consistent state - repairs are an essential part of operating a {{software}} cluster.

{{repairsFound}}

### Failed Repair Sessions

{{#hitLimit}}
There are more than {{hitLimit}} failed repair messages within the logs.
{{/hitLimit}}
{{^hitLimit}}
{{#numWarnings}}
There are {{numWarnings}} failed repair messages in the logs.
{{/numWarnings}}
{{/hitLimit}}

Repairs can fail for a variety of reasons, the most common cause is that a node has not responded within the timeout period, causing the repair to fail. These repair failure messages can not provide the root cause, since it is not clear to the node repairing, why the other node did not respond.

{{^numWarnings}}
There were no failed repairs within the logs.
{{/numWarnings}}

{{#warningsByNode}}
Warnings by Node:

{{warningsByNode}}

Warnings by Date, for the last {{numberOfDatesToReport}} dates of failure:

{{warningsByDate}}

{{/warningsByNode}}

### Incremental Repair
There are 2 main types of repair, full and incremental. Full repairs will operate over all the data,  while incremental repairs only operate on new data since the last incremental repair occurred.

When incremental repair is in use, sstables are split into two sets - repaired and unrepaired. This results in twice as many compactions since the repaired and unrepaired data can not be compacted together and combined.

Incremental repairs will never re-repair the same data again, which also means the file corruption and data loss through bugs and errors can not be repaired.

{{#isDse}}
DSE supports sub-range repairs which allow a repair to be divided up into a series of smaller repairs. This allows a repair to be split into discrete chunks and only check data within the token range that is being repaired. 

If incremental repairs have been used, the flags used to indicate that incremental repair is in effect can be cleared using a nodetool command:

`nodetool mark_unrepaired keyspace_name [table_name]`

After running the command, you will be able to use full repairs again.

{{/isDse}}

{{#isCassandra}}
We do not recommend that you use incremental repairs until Cassandra 4.x - some significant issues exist within incremental repair which were fixed in 4.0 (https://issues.apache.org/jira/browse/CASSANDRA-9143). A detailed description of the incremental repair process and issues has been published as a blog : https://thelastpickle.com/blog/2017/12/14/should-you-use-incremental-repair.html

Reaper provides the ability to perform sub-range repairs which allow a repair to be divided up into a series of smaller repairs. This allows a repair to be split into discrete chunks and only check data within the token range that is being repaired.

If incremental repairs have been used, the flags used to indicate that incremental repair is in effect can be cleared using a sstablerepairedset command (https://cassandra.apache.org/doc/latest/cassandra/tools/sstable/sstablerepairedset.html). Each node must be stopped before this command is run on it, and restarted afterwards.

`sstablerepairedset --really-set <options> [-f <sstable-list> | <sstables>]`

{{/isCassandra}}

{{incremental}}