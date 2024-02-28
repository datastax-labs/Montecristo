## JVM Garbage Collection

There are two valid options for garbage collection with {{software}}:

* **Par New + CMS**: The default garbage collector algorithms.  They can be optimized for low latency and high throughput but require a deep understanding of them and can be tricky to tune.
* **G1GC**: A collector optimized for high throughput, with minimal configuration, but typically displays higher latencies.

The overall distribution of pause times is as follows:

{{sortedDistributionTable}}

Pause times by day, broken down by time spent:

{{gcPauseTimesByDay}}

