## Compression settings

Compression can reduce the on-disk size, which can also improve performance for the read path when sized correctly.  Too large of a `chunk_length_in_kb` setting can result in reading significantly more data off disk than is required.  Compression is set using the table property compression.

For read heavy workloads, we typically recommend using a smaller chunk_length_in_kb to avoid read amplification.  This comes at a tradeoff of using extra memory off heap.  A `chunk_length_in_kb: 32` will use twice the memory as using `chunk_length_in_kb: 64` 

LZ4 Compression was added to Cassandra in version 1.2.2 via CASSANDRA-5038. It is the fastest compression available with results that are similar to the SnappyCompressor. Currently LZ4Compressor is the default algorithm.

### Table Compression Configuration


{{tablesUsingCompression}}


### Read Heavy Tables

The following tables are read heavy, meaning they have a 10:1 read:write ratio, as per the internally tracked metrics.

Tables with small partitions benefit from a smaller chunk length because it allows {{software}} to pull less bytes off disk, and reduces GC overhead by allocating few objects.  The entire chunk must be decompressed before it can be read.

{{readHeavy}}

{{#showChunkLengthKBNote}}
---

_**Noted for reference**: `chunk_length_kb` was renamed in 3.0 to `chunk_length_in_kb` for consistency with other parameters._

---
{{/showChunkLengthKBNote}}
