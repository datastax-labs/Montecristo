## Thread Pools

Cassandra uses a [Staged Event Driven Architecture](https://en.wikipedia.org/wiki/Staged_event-driven_architecture) that uses thread pools for common tasks. The read and the write paths have their own thread pools, where the number of threads can be controlled through configuration. Typically the number of threads is increased to put more pressure on the storage system and allow it to re-order commands in the most efficient way possible. The size of the read and write thread pools are controlled by the `concurrent_reads`, `concurrent_writes` `concurrent_counter_writes`, `concurrent_batchlog_writes` and `concurrent_materialized_view_writes` configuration settings.
  

{{concurrentReads}}

{{concurrentWrites}}

{{concurrentCounterWrites}}


