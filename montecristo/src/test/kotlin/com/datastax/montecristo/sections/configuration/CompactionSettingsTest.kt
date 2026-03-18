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

import com.datastax.montecristo.fileLoaders.parsers.application.CassandraYamlParser
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Schema
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class CompactionSettingsTest {

    @Test
    fun getDocumentNoRecsValues() {
        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 128
            concurrent_compactors: 16
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("concurrent_compactors: 16")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 128")
    }

    @Test
    fun getDocumentLowThroughputPerCompactorValues() {
        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 100
            concurrent_compactors: 16
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend giving at least 8 MB/s of throughput to each compactor in order to avoid heap pressure due to excessive throttling.")
        assertThat(template).contains("concurrent_compactors: 16")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 100")
    }

    @Test
    fun getDocumentDefaultCompactionsMb() {
        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 16
            concurrent_compactors: 2
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend increasing the value of compaction throughput to 64MB/s, unless Cassandra is running on a single spinning disk. The default compaction throttling to 16MB/s is usually too low for write-heavy workloads and modern hardware can handle higher values.")
        assertThat(template).contains("concurrent_compactors: 2")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 16")
    }

    @Test
    fun getDocumentUnthrottledCompactionsMb() {
        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 0
            concurrent_compactors: 2
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("The compaction is currently un-throttled. We recommend decreasing this value to 64MB/s for clusters using SSD storage and 16MB/s for clusters using spinning disks.")
        assertThat(template).contains("concurrent_compactors: 2")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 0")
    }

    @Test
    fun getDocumentVeryHighCompactionsMb() {

        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 400
            concurrent_compactors: 2
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("The compaction throughput has been set to 400.0 MB/s, which is unusually high. We recommend reviewing the reason that the setting was altered to be this high.")
        assertThat(template).contains("concurrent_compactors: 2")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 400")
    }


    @Test
    fun getDocumentVeryHighCompactionsMbNewYamlSetting() {

        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput: 400MB/s
            concurrent_compactors: 2
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest50(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())
        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("The compaction throughput has been set to 400.0 MB/s, which is unusually high. We recommend reviewing the reason that the setting was altered to be this high.")
        assertThat(template).contains("concurrent_compactors: 2")
        assertThat(template).contains("compaction_throughput: 400")
    }
}