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

import com.datastax.montecristo.helpers.Utils
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
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class AggregationQueriesTest {

    @Test
    fun getDocumentNoAggregationWarning() {
        val entries : MutableList<LogEntry> = mutableListOf()

        val mapOfDurations = mutableMapOf<String,Pair<String, String>>()
        mapOfDurations.put("node1", Pair("2020-01-01T00:00:00", "2020-01-30T23:59:59"))

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)

        every { cluster.metricServer } returns metricsServer
        every { cluster.metricServer.getLogDurations() } returns mapOfDurations
        val executionProfile = ExecutionProfile.default()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Aggregation +query", LogLevel.WARN,executionProfile.limits.aggregationWarnings) } returns entries

        val aggregations = AggregationQueries()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = aggregations.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("No aggregation warnings detected.")
    }

    @Test
    fun getDocumentSingleAggregationWarning() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("WARN", "Aggregation query used without partition key (ks: test_ks, tbl: tbl_test)", "20200723184102", "node1")
        entries.add(logEntry)

        val mapOfDurations = mutableMapOf<String,Pair<String, String>>()
        mapOfDurations.put("node1", Pair("2020-01-01T00:00:00", "2020-01-30T23:59:59"))

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.metricServer.getLogDurations() } returns mapOfDurations
        val executionProfile = ExecutionProfile.default()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Aggregation +query", LogLevel.WARN,executionProfile.limits.aggregationWarnings) } returns entries

        val aggregations = AggregationQueries()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = aggregations.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("test_ks.tbl_test|1")
        assertThat(template).contains("2020-07-23|1")
    }

    @Test
    fun getDocumentSingleAggregationWarningNoKSTable() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("WARN", "Aggregation query used on multiple partition keys (IN restriction)", "20200723184102", "node1")
        entries.add(logEntry)

        val mapOfDurations = mutableMapOf<String,Pair<String, String>>()
        mapOfDurations.put("node1", Pair("2020-01-01T00:00:00", "2020-01-30T23:59:59"))

        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.metricServer.getLogDurations() } returns mapOfDurations
        val executionProfile = ExecutionProfile.default()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Aggregation +query", LogLevel.WARN,executionProfile.limits.aggregationWarnings) } returns entries

        val aggregations = AggregationQueries()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = aggregations.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("unknown|1")
        assertThat(template).contains("2020-07-23|1")
    }

    @Test
    fun getDocumentMultipleAggregationWarning() {

        val logEntry = LogEntry("WARN", "Aggregation query used without partition key (ks: test_ks, tbl: tbl_test)", "20200723184102", "node1")
        // 5000 entries, for a single node, with 30 days of logs, ~ 6.9 per hour, above the threshold of 5 per hour.
        val entries : MutableList<LogEntry> = MutableList(5000) { logEntry }

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)

        val executionProfile = ExecutionProfile.default()
        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Aggregation +query", LogLevel.WARN,executionProfile.limits.aggregationWarnings) } returns entries

        val aggregations = AggregationQueries()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = aggregations.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).contains("test_ks.tbl_test|5000")
        assertThat(template).contains("2020-07-23|5000")
        assertThat(recs[0].longForm).contains("There are over 5 aggregation queries occurring on average every hour. We recommend investigating the purpose and source of these queries, and investigating alternative approaches to the business need they are used to address.")
    }

    @Test
    fun getDocumentMultipleAggregationWarningReducedLimitOnExecutionProfile() {

        val logEntry = LogEntry("WARN", "Aggregation query used without partition key (ks: test_ks, tbl: tbl_test)", "20200723184102", "node1")
        // 5000 entries, for a single node, with 30 days of logs, ~ 6.9 per hour, above the threshold of 5 per hour.
        val entries : MutableList<LogEntry> = MutableList(5000) { logEntry }

        val mapOfDurations = mutableMapOf<String,Double>()
        mapOfDurations.put("node1", 30*24.0)
        val executionProfile = ExecutionProfile(Limits(90,5000))
        val metricsServer = mockk<IMetricServer>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer } returns metricsServer
        every { cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays) } returns mapOfDurations

        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Aggregation +query", LogLevel.WARN,executionProfile.limits.aggregationWarnings) } returns entries

        val aggregations = AggregationQueries()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = aggregations.getDocument(cluster, searcher, recs, executionProfile)
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).contains("test_ks.tbl_test|5000")
        assertThat(template).contains("2020-07-23|5000")
        assertThat(template).contains("We found more than ${Utils.humanReadableCount(executionProfile.limits.aggregationWarnings.toLong())} log messages")
        assertThat(recs[0].longForm).contains("There are over 5.0 k aggregation query warnings within the logs. We recommend investigating the purpose and source of these queries, and investigating alternative approaches to the business need they are used to address.")

    }
}