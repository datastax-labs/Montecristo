## Version
{{#multipleVersions.isEmpty}}
The version of {{software}} currently in use is {{version}}.
{{/multipleVersions.isEmpty}}

{{^multipleVersions.isEmpty}}
Multiple different versions of {{software}} were detected, with {{version}} being the most common currently in use.

The full list of versions detected are:

{{multipleVersions}}

{{/multipleVersions.isEmpty}}
{{#showC4Upgrade}}
---

_**Noted for reference:** When upgrading, you must not skip major releases e.g. a 1.2 cluster must first be upgraded to 2.2, a 2.x and 3.0 cluster must first be upgraded to 3.11, before a final upgrade to 4.0. When upgrading, clusters should be upgraded to the latest minor release for the current version, before upgrading to the next major release._

---

### Cassandra 4
Cassandre 4 brings a huge range of enhancements, fixes, and new features. Some of the most important include:

#### Auditing
  
Cassandra 4 includes the ability to perform auditing, supporting regulatory compliance for the database. This feature is in addition to the Full Query Logging and CDC that is available.

#### Full Query Logging

Full Query logging logs all successful CQL requests, allowing for easier debugging, performance benchmarking, testing and replay of production queries.

#### Java 11

Cassandra 4 can run on Java 8 or Java 11, it is the first version of Cassandra to support Java 11. The use of a more recent JDK version brings a range of JVM improvements and allows the use of alternative GC algorithms such as Shenandoah and ZGC. Both Shenandoah and ZGC are significantly easier to tune and benefit from extremely low GC Stop-the-world pause times - this directly impacts tail-latencies and overall performance / throughput of the nodes within a cluster.

#### Virtual Tables

Internal data in Cassandra for management and metadata was previously available via JMX or through nodetool commands. Cassandra 4 now allows some of this information to be accessed via virtual tables, simplifying operational management.

{{/showC4Upgrade}}
{{#showC5Upgrade}}
---

_**Noted for reference:** When upgrading, you must not skip major releases e.g. a 2.2 cluster must first be upgraded to 3.11, and then to 4.1, before a final upgrade to 5.0. When upgrading, clusters should be upgraded to the latest minor release for the current version, before upgrading to the next major release._

---

### HCD / Cassandra 5.0
Cassandra 5 brings a huge range of enhancements, fixes, and new features. Some of the most important include:

#### Total Cost Ownership

Apache Cassandra version 5.0 and HCD provides significant performance improvements on throughput of requests, p99 latencies of requests, and its ability to store more data per node.  In many situations this can easily result in halving the TCO of the cluster.

Compared to Cassandra 3.x and DSE 6.8, benchmarking has demonstrated:
 - 3x the throughput on both the KeyValue and LWT (Light Weight Transaction / Paxos) workloads.  
 - Up to 10x reduction of p99 latencies on the IoT time-series workload under saturation.
 - Double the throughput limit of Light Weight Transaction requests.
 - Up to 5x node density.

#### Storage Attached Indexes

Our new Storage Attached Indexes provide lightning-fast data retrieval, allowing you to access the information you need with unprecedented speed and efficiency. [Watch this video about Storage Attached Indexes](https://www.youtube.com/watch?v=aBuIQSLxtnk&list=PLqcm6qE9lgKKls90MlpejceYUU_0qVnWa&index=12&t=1s).

#### Trie Memtables and Trie SSTables

Cassandra 5.0 includes Trie-based Memtables and SSTables, optimizing memory and storage usage, and ultimately improving the performance of your database. [Watch this video about Trie Memtables](https://www.youtube.com/watch?v=eKxj6s4vzmI&list=PLqcm6qE9lgKKls90MlpejceYUU_0qVnWa&index=13).

#### New Mathematical CQL Functions

Take advantage of new mathematical CQL functions, including abs, exp, log, log10, and round, to perform complex calculations within your queries.

#### Unified Compaction Strategy

Streamline data organization and management, enhancing the efficiency and performance of your clusters.

#### Vector Search

Cassandra 5.0 introduces vector search, which leverages storage-attached indexing and dense indexing techniques to transform data exploration and analysis. This update is especially impactful for domains like machine learning and artificial intelligence (AI). [Watch this video about Vector Search](https://www.youtube.com/watch?v=Bxc-JLRx450&list=PLqcm6qE9lgKKls90MlpejceYUU_0qVnWa&index=10).

DSE 6.9 and HCD offer newer, more performant with higher recall, JVector versions.

#### Dynamic Data Masking

Cassandra 5.0 introduces new dynamic data masking (DDM) capabilities which allow you to obscure sensitive information using a concept called masked columns. DDM doesn’t change the stored data. Instead, it just presents the data in its redacted form during SELECT queries.

#### JDK 17

JDK17 offers significant G1GC improvements that should be taken advantage of.  G1GC remains the recommended GC for Cassandra.  This likely will remain the recommendation until Generational ZGC is available.

#### Other

[Explore other exciting features](https://cassandra.apache.org/doc/trunk/cassandra/new/index.html#new-features-in-apache-cassandra-5-0) like more guardrails, extended TTL, new vector data types, tools for large partition identification, support for Microsoft Azure, a CIDR authorizer, pluggable crypto providers, and a virtual table for system logs, among others.
{{/showC5Upgrade}}
