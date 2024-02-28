## Batches

Batches can be used to group together queries.  Batches provide a guarantee all are eventually executed, but do not provide the semantics of a relational database such as atomicity (all or nothing) or isolation. They are most useful for queries on related tables (such as multiple views of the same data) but come at a significant overhead. We recommend limiting the usage of batches to only a few queries.

Large batches can cause significant GC pauses and result in cluster instability.  In the case of a single query in a batch failing, the entire batch will fail and must be retried.

In the case of large clusters, two unavailable nodes can cause an entire batch to fail, resulting in a rapidly growing batch log and hint pileup.  This can lead to a snowball effect resulting in additional downtime.  

Single partition batches do not have the overhead of multi-partition batches. They do not require the batch log and are executed atomically and in isolation as a single mutation.

{{findingsSummary}}
