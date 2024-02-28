/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.fileLoaders.parsers.schema.Field
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.nodetool.Info
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import com.datastax.montecristo.utils.HumanCount
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class CachesTest {

    private val nodetoolInfoTxt = """
ID                     : a770ae65-3127-4185-82f9-5aabea937668
Gossip active          : true
Thrift active          : false
Native Transport active: true
Load                   : 60.85 GiB
Generation No          : 1598553190
Uptime (seconds)       : 3423760
Heap Memory (MB)       : 2892.96 / 8032.00
Off Heap Memory (MB)   : 419.71
Data Center            : katiceva
Rack                   : RAC1
Exceptions             : 0
Key Cache              : entries 1039732, size 100 MiB, capacity 100 MiB, 24114077646 hits, 26253696380 requests, 0.919 recent hit rate, 14400 save period in seconds
Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 50 MiB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
Chunk Cache            : entries 7680, size 480 MiB, capacity 480 MiB, 4669097130 misses, 28489338168 requests, 0.836 recent hit rate, 87.740 microseconds miss latency
Percent Repaired       : 0.0%
Token                  : (invoke with -T/--tokens to see all 256 tokens)

 nodetool rc=0
"""


    private val cacheTemplate = """## Caches

Cassandra uses various caches to speed up various queries.  They are all stored off heap and can significantly improve the performance of reads when the hit rate is high, over 90%.  

When using a package install, caches are saved to `/var/lib/cassandra/saved_caches`.

The aggregated cache size and hit rates can be found in `nodetool info`:
```
ID                     : a770ae65-3127-4185-82f9-5aabea937668
Gossip active          : true
Thrift active          : false
Native Transport active: true
Load                   : 60.85 GiB
Generation No          : 1598553190
Uptime (seconds)       : 3423760
Heap Memory (MB)       : 2892.96 / 8032.00
Off Heap Memory (MB)   : 419.71
Data Center            : katiceva
Rack                   : RAC1
Exceptions             : 0
Key Cache              : entries 1039732, size 100 MiB, capacity 100 MiB, 24114077646 hits, 26253696380 requests, 0.919 recent hit rate, 14400 save period in seconds
Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 50 MiB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
Chunk Cache            : entries 7680, size 480 MiB, capacity 480 MiB, 4669097130 misses, 28489338168 requests, 0.836 recent hit rate, 87.740 microseconds miss latency
Percent Repaired       : 0.0%
Token                  : (invoke with -T/--tokens to see all 256 tokens)
```


There are also operating system level file caches, usually referred to as page cache.

### Page Cache

The page cache usage on the nodes we analyzed ranges from 1.05 MB to 1.05 MB

### Key Cache

Key cache settings

<span><div style="text-align:left;">Setting</div></span>|<span><div style="text-align:left;">Config Value</div></span>|<span><div style="text-align:left;">Description</div></span>
---|---|---
key_cache_keys_to_save|keys_to_save_value|Number of keys to cache
key_cache_save_period|500|Frequency to save key cache


This cache holds the location of keys.  It can save I/O and CPU time by avoiding bloom filter checks as well as partition index lookups.

### Row Cache

By default this is stored off heap.  A write to a partition invalidates the cache for an entire partition.  This can lead to significant overhead and wasted CPU cycles storing and invalidating caches.

#### Tables with Row Cache

No tables have row cache enabled.

### Counter Cache

The counter cache is used for both reads and writes.  This should always be enabled when using counters and sized to fit as many counters in memory as possible.  

<span><div style="text-align:left;">Setting</div></span>|<span><div style="text-align:left;">Config Value</div></span>|<span><div style="text-align:left;">Impact</div></span>
---|---|---
Counter Cache Size|25|Total Counter Cache In MB
Save Period|86400|Cache flush frequency


Tables Using Counters:

No tables were using counters.

---

_**Noted for reference:** If you are using counters heavily, speak to the DataStax services team to obtain further advice about tuning counter caches, and other caveats that come with the use of counters._

---"""

    @Test
    fun getDocument() {
        val tables: MutableList<Table> = mutableListOf()
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"

        every { table.operations } returns HumanCount(123)
        every { table.getRWRatioHuman() } returns "75%"
        every { table.liveDiskSpaceUsed.count.averageBytesAsHumanReadable() } returns "5.4 MB"
        every { table.caching.rows } returns "NONE"
        every { table.getRWRatio() } returns 25.0
        every { table.keyCacheHitRate.value.min() } returns 75.0
        tables.add(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val nodetoolInfo = mockk<Info>(relaxed = true)
        every { nodetoolInfo.rawContent } returns nodetoolInfoTxt

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cluster.getSetting("key_cache_keys_to_save", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_keys_to_save", mapOf(Pair("node1", ConfigValue(true, "","keys_to_save_value"))))
        every { cluster.getSetting("key_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","500"))))
        every { cluster.getSetting("counter_cache_size_in_mb", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_size_in_mb", mapOf(Pair("node1", ConfigValue(true, "","25"))))
        every { cluster.getSetting("counter_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","86400"))))

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.cached } returns 1024

        val node = ObjectCreators.createNode(nodeName = "node1", info = nodetoolInfo, cassandraYaml = cassandraYaml, config = config)
        val nodeList: List<Node> = listOf(node)

        every { cluster.nodes } returns nodeList

        val cache = Caches()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = cache.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(cacheTemplate) // Why am I failing?
    }

    @Test
    fun getDocumentTwoTables() {
        // This test is in preparation for when the ordering on the actual section matter, it looks
        // like it intends to, but doesn't.
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.getKeyspace() } returns "test"
        every { table1.rowCacheHit.count.sumAsHumanReadable() } returns "456"
        every { table1.rowCacheMiss.count.sumAsHumanReadable() } returns "654"
        every { table1.getRowCacheRatioHuman() } returns "65%"
        every { table1.caching.rows } returns "SOME"
        every { table1.getRWRatio() } returns 55.0
        every { table1.getReadLatencyP99() } returns 66.0

        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.getKeyspace() } returns "test"
        every { table2.rowCacheHit.count.sumAsHumanReadable() } returns "123"
        every { table2.rowCacheMiss.count.sumAsHumanReadable() } returns "321"
        every { table2.getRowCacheRatioHuman() } returns "75%"
        every { table2.caching.rows } returns "SOME"
        every { table2.getRWRatio() } returns 25.0
        every { table2.getReadLatencyP99() } returns 80.0
        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        every { cluster.schema.getUserTables() } returns tables

        val nodetoolInfo = mockk<Info>(relaxed = true)
        every { nodetoolInfo.rawContent } returns nodetoolInfoTxt

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.isRowCacheEnabled() } returns true

        every { cluster.getSetting("key_cache_keys_to_save", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_keys_to_save", mapOf(Pair("node1", ConfigValue(true, "","keys_to_save_value"))))
        every { cluster.getSetting("key_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","500"))))
        every { cluster.getSetting("counter_cache_size_in_mb", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_size_in_mb", mapOf(Pair("node1", ConfigValue(true, "","25"))))
        every { cluster.getSetting("counter_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","86400"))))

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.cached } returns 1024

        val node = ObjectCreators.createNode(nodeName = "node1", info = nodetoolInfo, cassandraYaml = cassandraYaml, config = config)
        val nodeList: List<Node> = listOf(node)

        every { cluster.nodes } returns nodeList


        val cache = Caches()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = cache.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(template).contains("test_table1|456|654|65%|66.0|55.0\n" +
                "test_table2|123|321|75%|80.0|25.0\n")
    }

    @Test
    fun getDocumentTwoTablesWithCounters() {
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.getKeyspace() } returns "test"
        every { table1.rowCacheHit.count.sumAsHumanReadable() } returns "456"
        every { table1.rowCacheMiss.count.sumAsHumanReadable() } returns "654"
        every { table1.getRowCacheRatioHuman() } returns "65%"
        every { table1.caching.rows } returns "SOME"
        every { table1.getRWRatio() } returns 55.0
        every { table1.getReadLatencyP99() } returns 66.0
        every { table1.fields } returns mapOf(Pair("a", Field("counter")),Pair("b", Field("counter")),Pair("c",Field("counter")))

        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.getKeyspace() } returns "test"
        every { table2.rowCacheHit.count.sumAsHumanReadable() } returns "123"
        every { table2.rowCacheMiss.count.sumAsHumanReadable() } returns "321"
        every { table2.getRowCacheRatioHuman() } returns "75%"
        every { table2.caching.rows } returns "SOME"
        every { table2.getRWRatio() } returns 25.0
        every { table2.getReadLatencyP99() } returns 80.0
        every { table2.fields } returns mapOf(Pair("a",Field("counter")))

        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        every { cluster.schema.getUserTables() } returns tables

        val nodetoolInfo = mockk<Info>(relaxed = true)
        every { nodetoolInfo.rawContent } returns nodetoolInfoTxt

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.isRowCacheEnabled() } returns true

        every { cluster.getSetting("key_cache_keys_to_save", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_keys_to_save", mapOf(Pair("node1", ConfigValue(true, "","keys_to_save_value"))))
        every { cluster.getSetting("key_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("key_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","500"))))
        every { cluster.getSetting("saved_caches_directory", ConfigSource.CASS) } returns ConfigurationSetting("saved_caches_directory", mapOf(Pair("node1", ConfigValue(true, "","test/directory"))))
        every { cluster.getSetting("counter_cache_size_in_mb", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_size_in_mb", mapOf(Pair("node1",ConfigValue(true, "", "25"))))
        every { cluster.getSetting("counter_cache_save_period", ConfigSource.CASS) } returns ConfigurationSetting("counter_cache_save_period", mapOf(Pair("node1", ConfigValue(true, "","86400"))))

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.cached } returns 1024

        val node = ObjectCreators.createNode(nodeName = "node1", info = nodetoolInfo, cassandraYaml = cassandraYaml, config = config)
        val nodeList: List<Node> = listOf(node)

        every { cluster.nodes } returns nodeList


        val cache = Caches()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = cache.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(template).contains("test_table1|3\ntest_table2|1\n")
    }
}