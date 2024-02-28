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