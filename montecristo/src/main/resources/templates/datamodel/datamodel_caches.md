## Caches

{{software}} uses various caches to speed up various queries.  They are all stored off heap and can significantly improve the performance of reads when the hit rate is high, over 90%.  

When using a package install, caches are saved to `/var/lib/cassandra/saved_caches`.

The aggregated cache size and hit rates can be found in `nodetool info`:
```
{{nodetoolInfoExample}}
```


There are also operating system level file caches, usually referred to as page cache.

### Page Cache

The page cache usage on the nodes we analyzed ranges from {{pageCacheMin}} to {{pageCacheMax}}

### Key Cache

Key cache settings

{{keyCacheTable}}

This cache holds the location of keys.  It can save I/O and CPU time by avoiding bloom filter checks as well as partition index lookups.

### Row Cache

By default this is stored off heap.  A write to a partition invalidates the cache for an entire partition.  This can lead to significant overhead and wasted CPU cycles storing and invalidating caches.

#### Tables with Row Cache

{{rowCacheTable}}

### Counter Cache

The counter cache is used for both reads and writes.  This should always be enabled when using counters and sized to fit as many counters in memory as possible.  

{{counterCacheTable}}

Tables Using Counters:

{{tablesWithCounters}}

---

_**Noted for reference:** If you are using counters heavily, speak to the DataStax services team to obtain further advice about tuning counter caches, and other caveats that come with the use of counters._

---