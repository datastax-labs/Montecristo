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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


internal class GossipLogPausesWarningsTest() {


    @Test
    fun testNoPauses() {
        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-15T12:13:14"))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("FailureDetector.java", limit = 1000000) } returns emptyList()
        val recs: MutableList<Recommendation> = mutableListOf()
        val pausesDoc = GossipLogPausesWarnings()
        val response = pausesDoc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 0 local pause warnings were discovered within the logs.")
    }

    @Test
    fun testSinglePause() {

        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-01T02:00:00"))

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "FailureDetector.java:278 - Not marking nodes down due to local pause of 6097678730 > 5000000000"
        val logEntry = LogEntry("WARN", example, "20200723184102", "node1")
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("FailureDetector.java", limit = 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val pausesDoc = GossipLogPausesWarnings()
        val response = pausesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 1 local pause warnings were discovered within the logs.")
        assertThat(response).contains("node1|1|6.10|6.10|3.05|0.08") // 6.1 seconds total / 2 hours duration, so its 3.1 seconds per hour, 0.08% of the hour.
    }

    @Test
    fun testDoublePauseSingleHour() {

        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-01T01:00:00"))

        val entries: MutableList<LogEntry> = mutableListOf()
        val example1 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 60000000000 > 5000000000"
        val logEntry1 = LogEntry("WARN", example1, "20200723184102", "node1")
        entries.add(logEntry1)
        val example2 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 70000000000 > 5000000000"
        val logEntry2 = LogEntry("WARN", example2, "20200723194102", "node1")
        entries.add(logEntry2)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("FailureDetector.java", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val pausesDoc = GossipLogPausesWarnings()
        val response = pausesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 2 local pause warnings were discovered within the logs.")
        assertThat(response).contains("node1|2|130.00|65.00|130.00|3.61") // 2 entries, 130 seconds total, 65 seconds average, 130 seconds in a single hour, 3.61% of time.
    }

    @Test
    fun testDoublePauseTwoHours() {

        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-01T02:00:00"))

        val entries: MutableList<LogEntry> = mutableListOf()
        val example1 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 60000000000 > 5000000000"
        val logEntry1 = LogEntry("WARN", example1, "20200723184102", "node1")
        entries.add(logEntry1)
        val example2 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 70000000000 > 5000000000"
        val logEntry2 = LogEntry("WARN", example2, "20200723204102", "node1")
        entries.add(logEntry2)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("FailureDetector.java", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val pausesDoc = GossipLogPausesWarnings()
        val response = pausesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 2 local pause warnings were discovered within the logs.")
        assertThat(response).contains("node1|2|130.00|65.00|65.00|1.81") // 2 entries, 130 seconds total, 65 seconds average, 65 seconds per hour, 1.81% of time.
    }

    @Test
    fun testTwoNodesPauses() {

        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-01T02:00:00"))
        durationMap.put("node2",Pair("2000-01-01T00:00:00", "2000-01-01T02:00:00"))

        val entries: MutableList<LogEntry> = mutableListOf()
        val example1 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 60000000000 > 5000000000"
        val logEntry1 = LogEntry("WARN", example1, "20200723184102", "node1")
        entries.add(logEntry1)
        val example2 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 70000000000 > 5000000000"
        val logEntry2 = LogEntry("WARN", example2, "20200723204102", "node1")
        entries.add(logEntry2)
        val example3 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 80000000000 > 5000000000"
        val logEntry3 = LogEntry("WARN", example3, "20200723194102", "node2")
        entries.add(logEntry3)
        val example4 = "FailureDetector.java:278 - Not marking nodes down due to local pause of 90000000000 > 5000000000"
        val logEntry4 = LogEntry("WARN", example4, "20200723214102", "node2")
        entries.add(logEntry4)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("FailureDetector.java", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val pausesDoc = GossipLogPausesWarnings()
        val response = pausesDoc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 4 local pause warnings were discovered within the logs.")
        assertThat(response).contains("node1|2|130.00|65.00|65.00|1.81") // 2 entries, 130 seconds total, 65 seconds average, 65 seconds per hour, 1.81% of time.
        assertThat(response).contains("node2|2|170.00|85.00|85.00|2.36") // 2 entries, 170 seconds total, 85 seconds average, 85 seconds per hour, 2.36% of time.
    }
}