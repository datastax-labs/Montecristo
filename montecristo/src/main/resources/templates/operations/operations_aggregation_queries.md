## Aggregation queries

CQL supports a number of aggregation operations, such as sum, count, average, min, max - when used on a single partition they are performant and present no issues. When the scope limit of the aggregation is a single partition, {{software}} can perform the query locally within a node and reads from a single partition.

When the aggregation extends to multiple partitions or even the whole table - the query will require the coordinator node to request the data for all the applicable partitions from nodes with replicas. The other nodes must read and transfer that data to the coordinator node which will process this data and perform the aggregation across the total data set.

These queries have two direct impacts on the cluster:

* The need to hold the values in memory prior to performing the aggregation will result in increased heap pressure on the node acting as the coordinator.
* The reading of the data requires many nodes to perform disk reads to satisfy the query.

{{software}} logs when queries sent to the server either aggregate across a whole table, or multiple partitions within a table.

{{#warningsByTable.empty}}
No aggregation warnings detected.
{{/warningsByTable.empty}}
{{^warningsByTable.isEmpty}}
{{^hitMessageLimit}}
There were {{numWarnings}} warnings within the logs.
{{/hitMessageLimit}}

Warnings by Table:

{{warningsByTable}}

Warnings by Date for the last 7 days that they appear:
{{warningsByDate}}
{{/warningsByTable.isEmpty}}

{{#hitMessageLimit}}
We found more than {{limit}} log messages of aggregation query warnings within the logs.
{{/hitMessageLimit}}