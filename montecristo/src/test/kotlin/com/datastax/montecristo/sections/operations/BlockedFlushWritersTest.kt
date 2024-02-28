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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


class BlockedFlushWritersTest {

    private val notSetTemplate = "## Blocked flush writers\n" +
            "\n" +
            "The Flush Writer thread pool is used to write Memtables to disk to create SSTables. When the queue for the thread pool is full, mutation threads (used for writes) will block until the disk activity has caught up. This will cause write latency to spike, as Coordinators will still be accepting mutations and sending messages to nodes.  \n" +
            "\n" +
            "\n" +
            "Currently `memtable_flush_writers` is not set in configuration, which defaults to one flush writer per data directory.\n" +
            "\n" +
            "\n" +
            "Blocked flush writers may occur due to:\n" +
            "\n" +
            "- Disk IO not keeping up with requirements.\n" +
            "- Incorrect configuration of the flush system.\n" +
            "- Data model edge cases where multiple Tables must flush at the same time due to commit log segment recycling.\n" +
            "- Use of nodetool flush or nodetool snapshot requiring multiple tables to flush at the same time.\n" +
            "- Blocked flush writers are found using the nodetool tpstats tool.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Node</div></span>|<span><div style=\"text-align:left;\">Blocked Flush Writers</div></span>\n" +
            "---|---\n" +
            "test_table|123\n" +
            "\n"

    private val setTemplate = "## Blocked flush writers\n" +
            "\n" +
            "The Flush Writer thread pool is used to write Memtables to disk to create SSTables. When the queue for the thread pool is full, mutation threads (used for writes) will block until the disk activity has caught up. This will cause write latency to spike, as Coordinators will still be accepting mutations and sending messages to nodes.  \n" +
            "\n" +
            "\n" +
            "`memtable_flush_writers` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "memtable_flush_writers: 4 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "Blocked flush writers may occur due to:\n" +
            "\n" +
            "- Disk IO not keeping up with requirements.\n" +
            "- Incorrect configuration of the flush system.\n" +
            "- Data model edge cases where multiple Tables must flush at the same time due to commit log segment recycling.\n" +
            "- Use of nodetool flush or nodetool snapshot requiring multiple tables to flush at the same time.\n" +
            "- Blocked flush writers are found using the nodetool tpstats tool.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Node</div></span>|<span><div style=\"text-align:left;\">Blocked Flush Writers</div></span>\n" +
            "---|---\n" +
            "test_table|123\n" +
            "\n"

    @Test
    fun getDocumentFlushWritersNotSet() {

        val node1 = ObjectCreators.createNode(nodeName = "node1")
        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "") } returns ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1", ConfigValue(false, "",""))))

        val tableMap = mutableMapOf<String, Long>()
        tableMap.put("test_table", 123)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getBlockedTaskCounts("MemtableFlushWriter") } returns tableMap
        val blockedTasks = BlockedTasks(metricsServer)

        every { cluster.blockedTasks } returns blockedTasks

        val tpStats = BlockedFlushWriters()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tpStats.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(notSetTemplate)
    }

    @Test
    fun getDocumentFlushWritersSet() {

        val node1 = ObjectCreators.createNode(nodeName = "node1")
        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "") } returns ConfigurationSetting("memtable_flush_writers", mapOf(Pair("node1",ConfigValue(true, "", "4"))))

        val tableMap = mutableMapOf<String, Long>()
        tableMap.put("test_table", 123)
        every { cluster.blockedTasks.memtableFlushWriters } returns tableMap

        val tpStats = BlockedFlushWriters()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tpStats.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(setTemplate)
    }
}