## Dropped Messages

{{software}} logs when messages are dropped, via the same mechanism as dropped messages reported in nodetool tpstats, and records the state of the thread pools at the time.  This is logged in two different ways, via JMX metrics and within the log files.

The JMX metrics cover a smaller portion of time, designed for more real-time monitoring, while the log messages provide a history built up over time.

More dropped messages on a particular node may indicate an issue with hardware or network connectivity.

### JMX Metrics
* **Dropped Mutations**: There were a total of {{totalDroppedMutations}} dropped mutations across the cluster.  Dropped mutations can leave the cluster in a state where data is inconsistent between nodes.  Without a repair (either read repair or explicit repair via reaper or nodetool) there is a risk of data loss.

* **Dropped Reads**: Dropped reads are another sign of an overloaded server.

Here are the number of dropped messages per node within the JMX Metrics.

{{jmxDroppedMutations}}

### Logged Dropped Mutations

Here are the number of dropped messages per node within the log files provided.

{{logDroppedMutations}}

{{#hitMessageLimit}}
We found more than 1,000,000 log messages of dropped mutations found within the logs.
{{/hitMessageLimit}}