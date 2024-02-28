## Tombstone Warnings

When {{software}} receives a delete request it appends a special value to the data to be deleted, called a tombstone. A tombstone is a marker which indicates that the preceding data is to be considered deleted. A tombstone is also written if a TTL set on the data cell expires. The tombstone will persist for the period of time defined by `gc_grace_seconds`. This is a setting that each table in {{software}} has. After the `gc_grace_seconds` period the tombstone expires.

Prior to the tombstone expiry, if the associated data is compacted, the compaction process will drop the data when it writes out the new table data to disk. The compaction process will still persist the tombstone and only it will be written out to the new table data on disk. After a tombstone expires, it can potentially be dropped the next time the SSTable is compacted. Older data marked for deletion can be spread across multiple tables. Those SSTables that make up the older data must all be part of the compaction for the tombstone to be dropped. If one or more of those SSTables that forms the delete data is left out of the compaction, then the tombstone will remain.

If no compaction occurs during the period between the tombstone creation and expiry, both the data and expired tombstone will still exist. In this case, during the next compaction both the data marked for deletion and the associated tombstone could potentially be dropped. Once again the SSTables that make up the older data must all be part of the compaction for the tombstone and data to be dropped.

The tombstone expiry delay introduced by `gc_grace_seconds` is done to allow the consistency mechanisms to propagate the delete operation among replicas holding the given data. Using this process, data can eventually be reliably deleted from all replicas in the cluster.

When reading data from disk, {{software}} reads every value for the requested cell in order to pick the most recent one. This involves reading potentially an excessive number of tombstones which might cause memory pressure or even exhaust all memory available. To indicate a danger of this happening, {{software}} issues warning log lines.

{{#byDay}}
Per day tombstone warnings

{{byDay}}
{{/byDay}}
{{#byNode}}
Per node tombstone warnings

{{byNode}}
{{/byNode}}
{{#byTable}}
Per table tombstone warnings

{{byTable}}
{{/byTable}}
{{noWarnings}}