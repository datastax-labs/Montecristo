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

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.metrics.SSTableStatistics
import com.datastax.montecristo.model.metrics.SingleSSTableStatistic
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators.createNode
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class RepairSessionsTest {

    @Test
    fun getDocumentNoFailures() {
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("repair") } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("There were no failed repairs within the logs.")
    }


    @Test
    fun getDocumentSingleFailures() {
        val entries: MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node1")
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("repair", LogLevel.ERROR, 100000) } returns entries.toList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("There are 1 failed repair messages in the logs.")
        assertThat(response).contains("node1|1")
        assertThat(response).contains("2020-07-23|1")
    }

    @Test
    fun getDocumentTwoFailuresDifferentNodes() {
        val entries: MutableList<LogEntry> = mutableListOf()
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node2"))

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("repair", LogLevel.ERROR, 100000) } returns entries.toList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("There are 2 failed repair messages in the logs.")
        assertThat(response).contains("node1|1")
        assertThat(response).contains("node2|1")
        assertThat(response).contains("2020-07-23|2")
    }

    @Test
    fun getDocumentThreeNodesGapsInTimeDifferentNodes() {
        val entries: MutableList<LogEntry> = mutableListOf()
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200723184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200722184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200722184102" ,"node1"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200721184102" ,"node2"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200717184102" ,"node2"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200716184102" ,"node2"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200715184102" ,"node2"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200714184102" ,"node3"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200712184102" ,"node3"))
        entries.add(LogEntry("ERROR", "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:","20200712184102" ,"node3"))

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("repair", LogLevel.ERROR, 100000) } returns entries.toList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("There are 12 failed repair messages in the logs.")
        assertThat(response).contains("node1|5\nnode2|4\nnode3|3") // shows the ordering is being used
        assertThat(response).contains("2020-07-23|3\n2020-07-22|2") // just shows the ordering is being used
        assertThat(response).contains("2020-07-22|2")
        assertThat(response).contains("2020-07-21|1")
        assertThat(response).contains("2020-07-17|1")
        assertThat(response).contains("2020-07-16|1")
        assertThat(response).contains("2020-07-15|1")
        assertThat(response).contains("2020-07-14|1")
        assertThat(response).contains("2020-07-12|2")
    }

    @Test
    fun getDocumentTriggerRecommendation () {
        val entries: MutableList<LogEntry> = mutableListOf()
        for (i in 1..150) {
            entries.add(
                LogEntry(
                    "ERROR",
                    "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:",
                    "20200723184102",
                    "node1"
                )
            )
            entries.add(
                LogEntry(
                    "ERROR",
                    "ERROR [Repair-Task:1] 2022-01-22 13:42:15,243  RepairRunnable.java:340 - Repair failed:",
                    "20200723184102",
                    "node2"
                )
            )
        }

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("repair", LogLevel.ERROR, 100000) } returns entries.toList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(response).contains("There are 300 failed repair messages in the logs.")
        assertThat(response).contains("node1|150")
        assertThat(response).contains("node2|150")
        assertThat(response).contains("2020-07-23|300")
    }

    @Test
    fun getDocumentNoRepairs() {
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.count() } returns 2
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("RepairSession.java", LogLevel.INFO, 1) } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that repairs are run on the cluster to ensure data consistency across the nodes.")
    }

    @Test
    fun getDocumentNoRepairsSingleNode() {
        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap["node1"] = Pair("2020-01-01T00:00:00", "2020-01-01T00:00:00")
        durationMap["node2"] = Pair("2020-01-02T12:00:00", "2020-01-05T00:00:00")
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.count() } returns 1
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("RepairSession.java", LogLevel.INFO, 1) } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentIncrementalFound() {
        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap["node1"] = Pair("2020-01-01T00:00:00", "2020-01-01T00:00:00")
        durationMap["node2"] = Pair("2020-01-02T12:00:00", "2020-01-05T00:00:00")
        val node = createNode("node1", ssTableStatistics = SSTableStatistics(listOf(SingleSSTableStatistic("k.t",1,true))))
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns listOf(node)
        every { cluster.databaseVersion.supportsIncrementalRepair() } returns false
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("RepairSession.java", LogLevel.INFO, 1) } returns listOf<LogEntry>(LogEntry("test","message","20200101-01-01","node1"))

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you do not use incremental repairs, and migrate to full repairs on all keyspaces.")
    }

    @Test
    fun getDocumentIncrementalFoundDBVersionAllowsIt() {
        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap["node1"] = Pair("2020-01-01T00:00:00", "2020-01-01T00:00:00")
        durationMap["node2"] = Pair("2020-01-02T12:00:00", "2020-01-05T00:00:00")
        val node = createNode("node1", ssTableStatistics = SSTableStatistics(listOf(SingleSSTableStatistic("k.t",1,true))))
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns listOf(node)
        every { cluster.databaseVersion.supportsIncrementalRepair() } returns true
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("RepairSession.java", LogLevel.INFO, 1) } returns listOf<LogEntry>(LogEntry("test","message","20200101-01-01","node1"))

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentNoRecentRepairsSingleNode() {
        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap["node1"] = Pair("2020-01-01T00:00:00", "2020-01-01T00:00:00")
        durationMap["node2"] = Pair("2020-01-02T12:00:00", "2020-01-25T00:00:00")
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.count() } returns 2
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("RepairSession.java", LogLevel.INFO, 1) } returns listOf<LogEntry>(LogEntry("test","message","20200101-01-01","node1"))

        val recs: MutableList<Recommendation> = mutableListOf()
        val repair = RepairSessions()
        val response = repair.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that repairs are run on the cluster to ensure data consistency across the nodes.")
    }
}