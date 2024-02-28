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
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class DroppedMessagesTest() {

    private val templateWithJmxDropActivity = "node_3|333|789\n" +
            "node_2|222|456\n" +
            "node_1|111|123\n" +
            "\n"

    @Test
    fun getDocumentDropMessage() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)
        droppedMutations.put("node_2", 222)
        droppedMutations.put("node_3", 333)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)
        droppedReads.put("node_2", 456)
        droppedReads.put("node_3", 789)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains(templateWithJmxDropActivity)
    }

    @Test
    fun getDocumentDropMessagesRemoveNodesWithNoDrops() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)
        droppedMutations.put("node_2", 222)
        droppedMutations.put("node_3", 0)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)
        droppedReads.put("node_2", 456)
        droppedReads.put("node_3", 0)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).doesNotContain("node_3")
    }

    @Test
    fun getDocumentNoDropMessage() {
        val droppedMessages = mutableMapOf<String, Long>()
        val droppedReads = mutableMapOf<String, Long>()
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getDroppedCounts("MUTATION") } returns droppedMessages
        every { cluster.metricServer.getDroppedCounts("READS") } returns droppedReads

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).doesNotContain(templateWithJmxDropActivity)
        assertThat(template).contains("There were no dropped mutations within the JMX metrics.")
    }


    @Test
    fun getDocumentLogsNoDroppedMessage() {
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getDroppedCounts("MUTATION") } returns emptyMap()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+MUTATION +messages +were +dropped +in +the +last",
                limit = 1000) } returns emptyList()
        every { cluster.metricServer.getDroppedCounts("READS") } returns emptyMap()

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("There were no dropped mutations within the JMX metrics.")
        assertThat(template).contains("There were no dropped mutations within the logs.")
    }

    @Test
    fun getDocumentSingleDropMessageInLogs() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)
        droppedMutations.put("node_2", 222)
        droppedMutations.put("node_3", 333)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)
        droppedReads.put("node_2", 456)
        droppedReads.put("node_3", 789)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)

        val logEntries = mutableListOf<LogEntry>()
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 1 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node1"))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+MUTATION +messages +were +dropped +in +the +last",
                limit = 1000000) } returns logEntries
        every { cluster.metricServer.getDroppedCounts("READS") } returns emptyMap()

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|30.00|1|1|1|0.00")
    }

    @Test
    fun getDocumentSingleDSEFormatDropMessageInLogs() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)
        droppedMutations.put("node_2", 222)
        droppedMutations.put("node_3", 333)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)
        droppedReads.put("node_2", 456)
        droppedReads.put("node_3", 789)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)
        mapOfDurations.put("node2", 30*24.0)
        mapOfDurations.put("node3", 30*24.0)

        val logEntries = mutableListOf<LogEntry>()
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in the last 5 s: 1 internal and 1 cross node. Mean internal dropped latency: 0 ms and Mean cross-node dropped latency: 0 ms", "20200101120100","node1"))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+MUTATION +messages +were +dropped +in +the +last",
            limit = 1000000) } returns logEntries
        every { cluster.metricServer.getDroppedCounts("READS") } returns emptyMap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|30.00|1|1|1|0.00")
    }

    @Test
    fun getDocumentMultipleDropMessageInLogsSingleNode() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1",  30*24.0)
        mapOfDurations.put("node2",  30*24.0)
        mapOfDurations.put("node3", 30*24.0)

        val logEntries = mutableListOf<LogEntry>()
        // 2 messages are in the same hour, so count of 5 messages, but only 4 hours with drops
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 1 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 2 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101130100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 3 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101140100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 4 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101150100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 5 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101150100","node1"))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations


        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+MUTATION +messages +were +dropped +in +the +last",
                limit = 1000000) } returns logEntries
        every { cluster.metricServer.getDroppedCounts("READS") } returns emptyMap()

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|30.00|5|15|4|0.03")
    }

    @Test
    fun getDocumentMultipleDropMessageInLogsMultiNode() {

        val droppedMutations = mutableMapOf<String, Long>()
        droppedMutations.put("node_1", 111)
        droppedMutations.put("node_2", 222)

        val droppedReads = mutableMapOf<String, Long>()
        droppedReads.put("node_1", 123)
        droppedReads.put("node_2", 456)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        every { metricsServer.getDroppedCounts("MUTATION") } returns droppedMutations
        every { metricsServer.getDroppedCounts("READ") } returns droppedReads

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1",  30*24.0)
        mapOfDurations.put("node2",  30*24.0)

        val logEntries = mutableListOf<LogEntry>()
        // node 1 : 2 messages are in the same hour, so count of 5 messages, but only 4 hours with drops
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 1 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 2 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101130100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 3 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101140100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 4 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101150100","node1"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 5 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101150100","node1"))

        // node 2 : 3 messages are in 1 hour, 2 in other
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 6 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node2"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 7 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node2"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 8 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101120100","node2"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 9 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101130100","node2"))
        logEntries.add(LogEntry("INFO","MUTATION messages were dropped in last 5000 ms: 1 internal and 10 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms", "20200101130100","node2"))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+MUTATION +messages +were +dropped +in +the +last",
                limit = 1000000) } returns logEntries
        every { cluster.metricServer.getDroppedCounts("READS") } returns emptyMap()

        val recs: MutableList<Recommendation> = mutableListOf()

        val droppedMessagesDoc = DroppedMessages()
        val template = droppedMessagesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|30.00|5|15|4|0.03")
        assertThat(template).contains("node2|30.00|5|40|2|0.06")
    }
}