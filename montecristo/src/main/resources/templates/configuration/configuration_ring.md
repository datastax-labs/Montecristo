## Token Ring

An unbalanced load on a cluster means that some nodes will contain more data than others. It can be caused by the following:

* Hot spots - by random chance one node ends up responsible for a higher percentage of the token space than the other nodes in the cluster.
* Manual Token Ranges - by incorrect allocation of tokens when a cluster has been set up with manually configured tokens.
* Node expansion - expandings the number of nodes while having a low number of vNodes configured
* Allocation Configuration - the allocation algorithm is unused. The algorithm can be set in 3.11 using `allocate_tokens_for_keyspace` and in 4.0 using `allocate_tokens_for_local_replication_factor`.

If a node contains disproportionately more tokens than other nodes in the cluster may experience one or more of the following issues:

* Run out storage more quickly than the other nodes.
* Serve more requests than the other nodes.
* Suffer from higher read and write latencies than the other nodes.
* Time to run repairs is longer than other nodes.
* Time to run compactions is longer than other nodes.
* Time to replace the node if it fails is longer than other nodes.

{{ring}}