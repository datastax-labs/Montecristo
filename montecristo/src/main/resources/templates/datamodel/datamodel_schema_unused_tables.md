## Unused Tables

A large number of tables in {{software}} can directly impact the performance of the cluster. Typically, you should have no more than 200 actively used tables in a cluster. Should the number of active unused tables increase above 500, the cluster may be prone to performance degradation and failure.

The problem arises because every table uses approximately 1 MB of memory for metadata. For each table acted on, a memtable representation is allocated. Tables with large amounts of data also increase pressure on memory by storing more data for the bloom filter and other auxiliary data structures.  
Also, each keyspace causes additional overhead in JVM memory. All of this together, impacts the performance of {{software}}. 

For that reason, it is recommended to drop tables that are unused.


{{unusedTables}}



---

**Noted for reference**: _We recommend dropping unused keyspaces and tables to facilitate operations._

---  

