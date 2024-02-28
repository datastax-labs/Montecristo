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
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class CompactionSettingsTest {

    @Test
    fun getDocumentNoRecsValues() {

        val compactorConfigSetting = ConfigurationSetting("compaction_throughput_mb_per_sec", mapOf(Pair("node1", ConfigValue(true, "16","128"))))
        val concurrentConfigSetting = ConfigurationSetting("concurrent_compactors", mapOf(Pair("node1", ConfigValue(true, "2","16"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16") } returns compactorConfigSetting
        every { cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2") } returns concurrentConfigSetting

        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("concurrent_compactors: 16")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 128")
    }

    @Test
    fun getDocumentLowThroughputPerCompactorValues() {

        val compactorConfigSetting = ConfigurationSetting("compaction_throughput_mb_per_sec", mapOf(Pair("node1", ConfigValue(true, "16","100"))))
        val concurrentConfigSetting = ConfigurationSetting("concurrent_compactors", mapOf(Pair("node1", ConfigValue(true, "2","16"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16") } returns compactorConfigSetting
        every { cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2") } returns concurrentConfigSetting

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
        val compactorConfigSetting = ConfigurationSetting("compaction_throughput_mb_per_sec", mapOf(Pair("node1", ConfigValue(true, "16","16"))))
        val concurrentConfigSetting = ConfigurationSetting("concurrent_compactors", mapOf(Pair("node1", ConfigValue(true, "2","2"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16") } returns compactorConfigSetting
        every { cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2") } returns concurrentConfigSetting

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

        val compactorConfigSetting = ConfigurationSetting("compaction_throughput_mb_per_sec", mapOf(Pair("node1", ConfigValue(true, "16","0"))))
        val concurrentConfigSetting = ConfigurationSetting("concurrent_compactors", mapOf(Pair("node1",ConfigValue(true, "2", "2"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16") } returns compactorConfigSetting
        every { cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2") } returns concurrentConfigSetting

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

        val compactorConfigSetting = ConfigurationSetting("compaction_throughput_mb_per_sec", mapOf(Pair("node1",ConfigValue(true, "16", "400"))))
        val concurrentConfigSetting = ConfigurationSetting("concurrent_compactors", mapOf(Pair("node1", ConfigValue(true, "2","2"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16") } returns compactorConfigSetting
        every { cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2") } returns concurrentConfigSetting

        val compactor = CompactionSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compactor.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("The compaction throughput has been set to 400 MB/s, which is unusually high. We recommend reviewing the reason that the setting was altered to be this high.")
        assertThat(template).contains("concurrent_compactors: 2")
        assertThat(template).contains("compaction_throughput_mb_per_sec: 400")
    }
}