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
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.profiles.Limits
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

class PreparedStatementsDiscardedTest {

    @Test
    fun getDocumentNoWarnings() {
        val entries : MutableList<LogEntry> = mutableListOf()

        val mapOfDurations = mutableMapOf<String,Pair<String, String>>()
        mapOfDurations.put("node1", Pair("2020-01-01T00:00:00", "2020-01-30T23:59:59"))

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)

        every { cluster.metricServer } returns metricsServer
        every { cluster.metricServer.getLogDurations() } returns mapOfDurations
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+prepared +statements +discarded", LogLevel.WARN,1000000) } returns entries

        val prepared = PreparedStatements()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = prepared.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).contains("No prepared statement discard warnings detected.")
    }

    @Test
    fun getDocumentSingleDiscardWarning() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("WARN", "181 prepared statements discarded in the last minute because cache limit reached (125 MB)", "20200723184102", "node1")
        entries.add(logEntry)

        val mapOfDurations = mutableMapOf<String,Pair<String, String>>()
        mapOfDurations.put("node1", Pair("2020-01-01T00:00:00", "2020-01-30T23:59:59"))

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.metricServer.getLogDurations() } returns mapOfDurations
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+prepared +statements +discarded", LogLevel.WARN,1000000) } returns entries

        val prepared = PreparedStatements()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = prepared.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).contains("2020-07-23|1")
    }

    @Test
    fun getDocumentMultipleDiscardWarnings() {

        val logEntry = LogEntry("WARN", "181 prepared statements discarded in the last minute because cache limit reached (125 MB)", "20200723184102", "node1")
        // 5000 entries, for a single node, with 30 days of logs, ~ 6.9 per hour, above the threshold of 1 per hour.
        val entries : MutableList<LogEntry> = MutableList(5000) { logEntry }

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+prepared +statements +discarded", LogLevel.WARN,1000000) } returns entries

        val prepared = PreparedStatements()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = prepared.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(template).contains("2020-07-23|5000")
        Assertions.assertThat(recs[0].longForm).contains("There are over 1 prepared statement discard warnings on average every hour. We recommend checking the prepared statement cache and identify incorrectly prepared statements from the application.")
    }

    @Test
    fun getDocumentOverLimitWarnings() {
        val logEntry = LogEntry("WARN", "181 prepared statements discarded in the last minute because cache limit reached (125 MB)", "20200723184102", "node1")
        // 5001 entries, for a single node, with 30 days of logs, ~ 6.9 per hour, above the threshold of 1 per hour.
        val entries : MutableList<LogEntry> = MutableList(5001) { logEntry }

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(ExecutionProfile.default().limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+prepared +statements +discarded", LogLevel.WARN,5000) } returns entries
        val executionProfile = ExecutionProfile(Limits(90,5000, 5000))

        val prepared = PreparedStatements()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = prepared.getDocument(cluster,searcher, recs, executionProfile)
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(template).contains("2020-07-23|5001")
        Assertions.assertThat(recs[0].longForm).contains("There are over 5.0 k prepared statement discard warnings within the logs.")
    }
}