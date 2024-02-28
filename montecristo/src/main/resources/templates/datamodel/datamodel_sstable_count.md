## SSTable Counts

The number of SSTables on a node for a given database table will vary based on a number of factors such as compaction strategy settings, data model, data sizes etc.

When a table is using Size Tiered Compaction Strategy (STCS) it is more sensitive to an excessive number of SSTables - it degrades read performance, causes heap pressure and creates performance issues with the IO Caches.

### High STCS SSTable Counts

{{sstablecounts}}

