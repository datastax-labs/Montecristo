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

package com.datastax.montecristo.sections.configuration

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


internal class MemtableStorageSettingsTest {

    private val heapTemplate = "## Memtable storage\n" +
            "\n" +
            "`memtable_allocation_type` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_allocation_type: heap_buffers \n" +
            "```\n" +
            "\n" +
            "`memtable_heap_space_in_mb` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_heap_space_in_mb: 2048 \n" +
            "```\n" +
            "\n" +
            "`memtable_offheap_space_in_mb` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_offheap_space_in_mb: 2048 \n" +
            "```\n" +
            "\n" +
            "`memtable_cleanup_threshold` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_cleanup_threshold: 1 \n" +
            "```\n" +
            "\n" +
            "`memtable_flush_writers` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_flush_writers: 4 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"


    private val offHeapTemplate = "## Memtable storage\n" +
            "\n" +
            "`memtable_allocation_type` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_allocation_type: offheap_buffers \n" +
            "```\n" +
            "\n" +
            "`memtable_heap_space_in_mb` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_heap_space_in_mb: 2048 \n" +
            "```\n" +
            "\n" +
            "`memtable_offheap_space_in_mb` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_offheap_space_in_mb: 2048 \n" +
            "```\n" +
            "\n" +
            "`memtable_cleanup_threshold` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_cleanup_threshold: 1 \n" +
            "```\n" +
            "\n" +
            "`memtable_flush_writers` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_flush_writers: 4 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentHeap3_11_7() {
        // Test that a 3.11+ cluster with offheap buffers enabled DOES get the offheap buffers recommendation.
        val memtableAllocationType = ConfigurationSetting("memtable_allocation_type", mapOf(Pair("node1",  ConfigValue(true, "heap_buffers","heap_buffers")), Pair("node2", ConfigValue(true, "heap_buffers","heap_buffers"))))
        val memtableHeapSpace = ConfigurationSetting("memtable_heap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableOffHeapSpace = ConfigurationSetting("memtable_offheap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableCleanupThreshold = ConfigurationSetting("memtable_cleanup_threshold", mapOf(Pair("node1", ConfigValue(true, "not set","1")), Pair("node2", ConfigValue(true, "not set","1"))))
        val memtableFlushWriters = ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1", ConfigValue(true, "not set","4")), Pair("node2",ConfigValue(true, "not set", "4"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers") } returns memtableAllocationType
        every { cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableHeapSpace
        every { cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableOffHeapSpace
        every { cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set") } returns memtableCleanupThreshold
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set") } returns memtableFlushWriters

        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.7")
        every { cluster.isDse } returns false

        val mem = MemtableStorageSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mem.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend storing memtables fully off heap to minimize heap pressure and benefit from up to 10% more write throughput. Use `offheap_objects` as `memtable_allocation_type` instead of `heap_buffers`.")
        assertThat(template).isEqualTo(heapTemplate)
    }

    @Test
    fun getDocumentOffHeap3_11_7() {
        // Test that a 3.11+ cluster with offheap buffers enabled does not get the offheap buffers recommendation.
        val memtableAllocationType = ConfigurationSetting("memtable_allocation_type", mapOf(Pair("node1", ConfigValue(true, "heap_buffers","offheap_buffers")), Pair("node2", ConfigValue(true, "heap_buffers","offheap_buffers"))))
        val memtableHeapSpace = ConfigurationSetting("memtable_heap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableOffHeapSpace = ConfigurationSetting("memtable_offheap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableCleanupThreshold = ConfigurationSetting("memtable_cleanup_threshold", mapOf(Pair("node1",ConfigValue(true, "not set", "1")), Pair("node2", ConfigValue(true, "not set","1"))))
        val memtableFlushWriters = ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1",ConfigValue(true, "not set", "4")), Pair("node2", ConfigValue(true, "not set","4"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers") } returns memtableAllocationType
        every { cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableHeapSpace
        every { cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableOffHeapSpace
        every { cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set") } returns memtableCleanupThreshold
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set") } returns memtableFlushWriters

        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.7")
        every { cluster.isDse } returns false

        val mem = MemtableStorageSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mem.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(offHeapTemplate)
    }

    @Test
    fun getDocumentHeap2_0_17() {
        // We don't mind if 2.x uses offheap buffers, but we don't recommend turning it on.
        val memtableAllocationType = ConfigurationSetting("memtable_allocation_type", mapOf(Pair("node1", ConfigValue(true, "heap_buffers","heap_buffers")), Pair("node2", ConfigValue(true, "heap_buffers","heap_buffers"))))
        val memtableHeapSpace = ConfigurationSetting("memtable_heap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableOffHeapSpace = ConfigurationSetting("memtable_offheap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableCleanupThreshold = ConfigurationSetting("memtable_cleanup_threshold", mapOf(Pair("node1", ConfigValue(true, "not set","1")), Pair("node2", ConfigValue(true, "not set","1"))))
        val memtableFlushWriters = ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1", ConfigValue(true, "not set","4")), Pair("node2", ConfigValue(true, "not set","4"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers") } returns memtableAllocationType
        every { cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableHeapSpace
        every { cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableOffHeapSpace
        every { cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set") } returns memtableCleanupThreshold
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set") } returns memtableFlushWriters

        every { cluster.databaseVersion } returns DatabaseVersion.fromString("2.0.17")
        every { cluster.isDse } returns false
        val mem = MemtableStorageSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mem.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(heapTemplate)
    }

    @Test
    fun getDocumentHeap5_0_1() {
        // 5.0.1 can't use off heap buffers do not recommend it.
        val memtableAllocationType = ConfigurationSetting("memtable_allocation_type", mapOf(Pair("node1", ConfigValue(true, "heap_buffers","heap_buffers")), Pair("node2", ConfigValue(true, "heap_buffers","heap_buffers"))))
        val memtableHeapSpace = ConfigurationSetting("memtable_heap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableOffHeapSpace = ConfigurationSetting("memtable_offheap_space_in_mb", mapOf(Pair("node1",ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableCleanupThreshold = ConfigurationSetting("memtable_cleanup_threshold", mapOf(Pair("node1", ConfigValue(true, "not set","1")), Pair("node2", ConfigValue(true, "not set","1"))))
        val memtableFlushWriters = ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1", ConfigValue(true, "not set","4")), Pair("node2", ConfigValue(true, "not set","4"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers") } returns memtableAllocationType
        every { cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableHeapSpace
        every { cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableOffHeapSpace
        every { cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set") } returns memtableCleanupThreshold
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set") } returns memtableFlushWriters

        every { cluster.databaseVersion } returns DatabaseVersion.fromString("5.0.1", true)
        every { cluster.isDse} returns true
        val mem = MemtableStorageSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mem.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentHeap5_1_0() {
        // 5.1.0 CAN use off heap buffers so we do recommend it.
        val memtableAllocationType = ConfigurationSetting("memtable_allocation_type", mapOf(Pair("node1", ConfigValue(true, "heap_buffers","heap_buffers")), Pair("node2", ConfigValue(true, "heap_buffers","heap_buffers"))))
        val memtableHeapSpace = ConfigurationSetting("memtable_heap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableOffHeapSpace = ConfigurationSetting("memtable_offheap_space_in_mb", mapOf(Pair("node1", ConfigValue(true, "not set","2048")), Pair("node2", ConfigValue(true, "not set","2048"))))
        val memtableCleanupThreshold = ConfigurationSetting("memtable_cleanup_threshold", mapOf(Pair("node1", ConfigValue(true, "not set","1")), Pair("node2", ConfigValue(true, "not set","1"))))
        val memtableFlushWriters = ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1", ConfigValue(true, "not set","4")), Pair("node2", ConfigValue(true, "not set","4"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers") } returns memtableAllocationType
        every { cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableHeapSpace
        every { cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set") } returns memtableOffHeapSpace
        every { cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set") } returns memtableCleanupThreshold
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set") } returns memtableFlushWriters
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("5.1.0")
        every { cluster.isDse} returns true
        val mem = MemtableStorageSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mem.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend storing memtables fully off heap to minimize heap pressure and benefit from up to 10% more write throughput. Use `offheap_objects` as `memtable_allocation_type` instead of `heap_buffers`.")
        assertThat(template).isEqualTo(heapTemplate)
    }
}