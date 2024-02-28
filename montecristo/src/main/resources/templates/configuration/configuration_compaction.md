## Compaction
  
The `concurrent_compactors` setting specifies how many compaction tasks may run in parallel. The default number in versions 1.2 and 2.0 uses the number of cores in the system. As of version 2.1 the default is the lower value of number of cores or number of disks. If set too high, this setting can result in premature tenuring in the JVM, as the compaction tasks are throttled by the `compaction_throughput_mb_per_sec`.  

{{compactionSettings}}


---

_**Noted for reference**: The compaction throughput is the total throughput shared between the compaction threads._

---

