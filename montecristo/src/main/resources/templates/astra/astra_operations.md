## Table operations

The following table contains read operations per hour for each of the tables within the database and a grand total - each for 4 different consistency levels, so that you can choose the appropriate value to the scenario.

If the values are available, the final column is populated with the sum of the co-ordinator request counts - which is going to be the most accurate figure of operation requests that can be provided and does not rely on ascertaining the consistency level being used against the table.

Read operations do not equate to Read Request Units (RRUs) for Astra - the size of the read in terms of physical kb size, or the number of rows selected is not recorded within the counts, so if for a given table the users select 10 rows, then the number of RRUs would be 10x the read operations / hour.

These figures account for local / global read repair / paxos counts on a per table basis.

In a single DC scenario, you can expect Quorum and Local Quorum to give the same values.

---

_**Noted for reference**: Row counts are only available when Cassandra 2.2 or above statistics files are present in the collected files. If the version is 2.1 or the files on the server are still in 2.1 format, no row count can be provided._

---

_**Noted for reference**: Spark range reads are not included within the Cassandra read counts. If an analytics workload is run on the cluster, the read counts will not be correct._

---

{{readOperations}}
