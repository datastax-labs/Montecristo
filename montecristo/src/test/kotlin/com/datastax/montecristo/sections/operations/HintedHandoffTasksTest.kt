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
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.nodetool.TpStats
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class HintedHandoffTasksTest {

    private val tpStatsTxt = "Pool Name                    Active   Pending      Completed   Blocked  All time blocked\n" +
            "MutationStage                     0         0    43179317360         0                 0\n" +
            "ReadStage                         0         0     4820054701         0                 0\n" +
            "RequestResponseStage              0         0    34260072109         0                 0\n" +
            "ReadRepairStage                   0         0       16666139         0                 0\n" +
            "CounterMutationStage              0         0              0         0                 0\n" +
            "MiscStage                         0         0              0         0                 0\n" +
            "AntiEntropySessions               0         0        1477340         0                 0\n" +
            "HintedHandoff                     0         0            417         0                 0\n" +
            "GossipStage                       0         0      263538956         0                 0\n" +
            "CacheCleanupExecutor              0         0              0         0                 0\n" +
            "InternalResponseStage             0         0           4150         0                 0\n" +
            "CommitLogArchiver                 0         0              0         0                 0\n" +
            "ValidationExecutor                0         0       16303362         0                 0\n" +
            "MigrationStage                    0         0             12         0                 0\n" +
            "CompactionExecutor                0         0       81240812         0                 0\n" +
            "AntiEntropyStage                  0         0       76490512         0                 0\n" +
            "PendingRangeCalculator            0         0             13         0                 0\n" +
            "Sampler                           0         0              0         0                 0\n" +
            "MemtableFlushWriter               0         0       18600473         0                 0\n" +
            "MemtablePostFlush                 0         0       30106576         0                 0\n" +
            "MemtableReclaimMemory             0         0       18600473         0                 0\n" +
            "Native-Transport-Requests         0         0    36715755916         0            190868\n" +
            "\n" +
            "Message type           Dropped\n" +
            "READ                         0\n" +
            "RANGE_SLICE                  0\n" +
            "_TRACE                       0\n" +
            "MUTATION               1233473\n" +
            "COUNTER_MUTATION             0\n" +
            "BINARY                       0\n" +
            "REQUEST_RESPONSE             0\n" +
            "PAGED_RANGE                  0\n" +
            "READ_REPAIR               1246"

    @Test
    fun getDocumentHintedHandoffAppears() {

        val tpStatsModel = mockk<TpStats>(relaxed = true)
        every { tpStatsModel.data } returns tpStatsTxt.split("\n")
        every { tpStatsModel.path} returns "extracted/node1_artifacts_2020_10_06_1140_1601977214"

        val node = ObjectCreators.createNode(nodeName = "node1", tpStats = tpStatsModel)
        val nodeList: List<Node> = listOf(node)

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 12.0)

        val logEntries = mutableListOf<LogEntry>()
        logEntries.add(LogEntry("INFO"," HintsDispatchExecutor.java:297 - Finished hinted handoff of file b17dd9ad-7041-497e-a86a-679db336001b-1619009281735-1.hints to endpoint /10.40.76.71: b17dd9ad-7041-497e-a86a-679db336001b, completed", "20210101060700","node1"))
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getNode(node.hostname)?.info?.getUptimeInHours() } returns 2.0
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Finished +hinted +handoff +to +endpoint", limit = 1000000)} returns logEntries

        val hintedHandoff = HintedHandoffTasks()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = hintedHandoff.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        // This is the TPStats row
        Assertions.assertThat(template).contains("node1|0|0|417|208.5")
        // 208.5 hints per hour, this will trigger the recommendation, even though the log entry does not.
        Assertions.assertThat(recs[0].shortForm).isEqualTo("The highest average hints per hours for a node is 208.5. A value this high indicates that the cluster is facing stability issues and should be investigated further.")
        // This is the log row - 1 entry in 12 hours, 0.08(25)
        Assertions.assertThat(template).contains("node1|1|0.08")
     }

    @Test
    fun getDocumentNoHintedHandoff() {

        val tpStatsModel = mockk<TpStats>(relaxed = true)
        val statsNoHintedHandoff = tpStatsTxt.split("\n").map { if (it.startsWith ("HintedHandoff")) { it.replace("417","0") } else { it }}.toList()
        every { tpStatsModel.data  } returns statsNoHintedHandoff
        every { tpStatsModel.path} returns "extracted/node1_artifacts_2020_10_06_1140_1601977214"

        val node = ObjectCreators.createNode(nodeName = "node1", tpStats = tpStatsModel)
        val nodeList: List<Node> = listOf(node)

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 12.0)

        val logEntries = emptyList<LogEntry>()
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getNode(node.hostname)?.info?.getUptimeInHours() } returns 2.0
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Finished +hinted +handoff +to +endpoint", limit = 1000000)} returns logEntries
        val dateMap :  Map<String, Pair<String,String>> = mapOf (Pair("node1", Pair("2021-01-01T00:00:00","2021-01-01T12:00:00")))

        val hintedHandoff = HintedHandoffTasks()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = hintedHandoff.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        // This is the TPStats row, no completed hints or hints per hr
        Assertions.assertThat(template).contains("node1|0|0|0|0")
        // No log entries, so gets the blank table result.
        Assertions.assertThat(template).contains("There were no hints dispatched within the log files.")

    }

    @Test
    fun getDocumentMultiDropMessageMoreThanRecThreshold() {

        val tpStatsModel = mockk<TpStats>(relaxed = true)
        val statsNoHintedHandoff = tpStatsTxt.split("\n").map { if (it.startsWith ("HintedHandoff")) { it.replace("417","0") } else { it }}.toList()
        every { tpStatsModel.data  } returns statsNoHintedHandoff
        every { tpStatsModel.path} returns "extracted/node1_artifacts_2020_10_06_1140_1601977214"

        val node = ObjectCreators.createNode(nodeName = "node1", tpStats = tpStatsModel)
        val nodeList: List<Node> = listOf(node)

        val logEntry = LogEntry("WARN", "faux message", "20200723184102", "node1")
        val logEntries = MutableList(500)  { logEntry } // 500 entries, 12 hours, 41.66 per hour

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 12.0)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getNode(node.hostname)?.info?.getUptimeInHours() } returns 2.0
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Finished +hinted +handoff +to +endpoint", limit = 1000000)} returns logEntries

        val hintedHandoff = HintedHandoffTasks()
        val recs: MutableList<Recommendation> = mutableListOf()
        val template = hintedHandoff.getDocument(cluster, searcher, recs,ExecutionProfile.default())

        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        Assertions.assertThat(recs[0].shortForm).isEqualTo("The highest average hints per hours for a node is 41.67. A value this high indicates that the cluster is facing stability issues and should be investigated further.")
    }
}