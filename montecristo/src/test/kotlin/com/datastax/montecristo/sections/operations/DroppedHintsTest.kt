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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class DroppedHintsTest() {
    private val dropMessage = "WARN [ScheduledTasks:1] 2020-10-16 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."

    private val singleDropTemplate = "## Dropped Hints\n" +
            "\n" +
            "The Hinted Handoff system only stores hints if a node has been down for less than `max_hint_window_in_ms` which defaults to 10800000 or 3 hours. When nodes are down for longer than the timeout, hints are no longer collected and the node will not receive missed writes. When the down node returns it will not receive hints for writes that occurred after the `max_hint_window_in_ms` expired.\n" +
            "\n" +
            "Total Dropped Hints: 12345\n" +
            "\n" +
            "Dropped Hints by Node:\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Host</div></span>|<span><div style=\"text-align:left;\">Hints Dropped</div></span>\n" +
            "---|---\n" +
            "node1|12345\n" +
            "\n" +
            "\n"

    private val multiDropTemplate = "## Dropped Hints\n" +
            "\n" +
            "The Hinted Handoff system only stores hints if a node has been down for less than `max_hint_window_in_ms` which defaults to 10800000 or 3 hours. When nodes are down for longer than the timeout, hints are no longer collected and the node will not receive missed writes. When the down node returns it will not receive hints for writes that occurred after the `max_hint_window_in_ms` expired.\n" +
            "\n" +
            "Total Dropped Hints: 61725\n" +
            "\n" +
            "Dropped Hints by Node:\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Host</div></span>|<span><div style=\"text-align:left;\">Hints Dropped</div></span>\n" +
            "---|---\n" +
            "node1|61725\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentSingleDropMessage() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("WARN", dropMessage, "20200723184102", "node1")
        entries.add(logEntry)
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("dropped AND hints", LogLevel.WARN,100000) } returns entries

        val droppedHints = DroppedHints()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = droppedHints.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        Assertions.assertThat(recs.first().shortForm).isEqualTo("Ensure repairs are running regularly as dropped hints were spotted.")
        Assertions.assertThat(template).isEqualTo(singleDropTemplate)
    }

    @Test
    fun getDocumentMultiDropMessage() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val dropMessage1 = "WARN [ScheduledTasks:1] 2020-10-16 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."
        val logEntry1 = LogEntry("WARN", dropMessage1, "20200723184102", "node1")
        entries.add(logEntry1)

        val dropMessage2 = "WARN [ScheduledTasks:1] 2020-10-17 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."
        val logEntry2 = LogEntry("WARN", dropMessage2, "20200723184102", "node1")
        entries.add(logEntry2)

        val dropMessage3 = "WARN [ScheduledTasks:1] 2020-10-18 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."
        val logEntry3 = LogEntry("WARN", dropMessage3, "20200723184102", "node1")
        entries.add(logEntry3)

        val dropMessage4 = "WARN [ScheduledTasks:1] 2020-10-19 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."
        val logEntry4 = LogEntry("WARN", dropMessage4, "20200723184102", "node1")
        entries.add(logEntry4)

        val dropMessage5 = "WARN [ScheduledTasks:1] 2020-10-20 13:56:44,486 HintedHandoffMetrics.java:108 - node1 has 12345 dropped hints, because node is down past configured hint window."
        val logEntry5 = LogEntry("WARN", dropMessage5, "20200723184102", "node1")
        entries.add(logEntry5)

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("dropped AND hints", LogLevel.WARN,100000) } returns entries

        val droppedHints = DroppedHints()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = droppedHints.getDocument(cluster, searcher, recs, ExecutionProfile.default())

        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        Assertions.assertThat(recs.first().shortForm).isEqualTo("Ensure repairs are running regularly as dropped hints were spotted.")
        Assertions.assertThat(template).isEqualTo(multiDropTemplate)
    }
}