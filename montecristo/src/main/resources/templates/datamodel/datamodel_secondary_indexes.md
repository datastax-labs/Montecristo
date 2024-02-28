## Secondary Indexes

The native secondary indexes in {{software}} act as reverse indexes. It indexes the data on each node by creating an internal table that maps specific values of the column into a full primary key. The aim is to support data access based on a condition that would be disallowed with the structure of the key as defined in the base table.

Secondary indexes have some limitations:

* Can only index a single regular column per index.
* Non-equality and range conditions are unsupported.
* Can be heavily impacted by the cardinality of the indexed column. If low cardinality exists, it can lead to creation of wide partitions. If high cardinality exists, it can lead to the creation of very small partitions. In either case, performance may suffer as described above.
* Deletions can impact their performance. A high number of tombstones in a secondary index severely degrades its performance.
* Secondary indexes are local to the node they're stored on. That is they index the base table data locally on the node only. Hence, they never follow the normal placement of a partition key in the cluster. If they are the only access criteria to the data without restriction on the partition key, they result in a scatter-gather request. Such a request involves querying all nodes in a datacenter which causes suboptimal performance.

For these reasons, secondary indexes must be used with great caution and designed out by denormalizing where possible. They might be a reasonable option when used to filter out results limited to a single partition of a table with small partitions and infrequent deletions. Even under these conditions, it is strongly recommended to thoroughly test queries that use secondary indexes with representative data and load.

Because {{software}} needs resources to build and maintain secondary indexes in a consistent state, DataStax recommends keeping the number as low as possible and removing all unused secondary indexes.

{{2i_in_use}}
