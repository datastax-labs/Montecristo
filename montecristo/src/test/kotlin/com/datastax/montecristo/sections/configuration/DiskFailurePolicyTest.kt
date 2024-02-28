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

internal class DiskFailurePolicyTest {

    @Test
    fun getDocumentConsistent() {
        val diskFailureConfigSetting = ConfigurationSetting("disk_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop","die")), Pair("node2", ConfigValue(true, "stop","die"))))
        val commitFailureConfigSetting = ConfigurationSetting("commit_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop","die")), Pair("node2", ConfigValue(true, "stop","die"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("disk_failure_policy", ConfigSource.CASS, "stop") } returns diskFailureConfigSetting
        every { cluster.getSetting("commit_failure_policy", ConfigSource.CASS, "stop") } returns commitFailureConfigSetting

        val disk = DiskFailurePolicy()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = disk.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("disk_failure_policy: die")
        assertThat(template).contains("commit_failure_policy: die")
    }


    @Test
    fun getDocumentConsistentStop() {
        val diskFailureConfigSetting = ConfigurationSetting("disk_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop","stop")), Pair("node2", ConfigValue(true, "stop","stop"))))
        val commitFailureConfigSetting = ConfigurationSetting("commit_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop","stop")), Pair("node2", ConfigValue(true, "stop","stop"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("disk_failure_policy", ConfigSource.CASS, "stop") } returns diskFailureConfigSetting
        every { cluster.getSetting("commit_failure_policy", ConfigSource.CASS, "stop") } returns commitFailureConfigSetting

        val disk = DiskFailurePolicy()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = disk.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend setting the `disk_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages.")
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[1].longForm).isEqualTo("We recommend setting the `commit_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages.")
        assertThat(template).contains("disk_failure_policy: stop")
        assertThat(template).contains("commit_failure_policy: stop")
    }

    @Test
    fun getDocumentInconsistentStop() {
        val diskFailureConfigSetting = ConfigurationSetting("disk_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop", "stop")), Pair("node2",  ConfigValue(true, "stop","die"))))
        val commitFailureConfigSetting = ConfigurationSetting("commit_failure_policy", mapOf(Pair("node1", ConfigValue(true, "stop", "stop")), Pair("node2",  ConfigValue(true, "stop","die"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("disk_failure_policy", ConfigSource.CASS, "stop") } returns diskFailureConfigSetting
        every { cluster.getSetting("commit_failure_policy", ConfigSource.CASS, "stop") } returns commitFailureConfigSetting

        val disk = DiskFailurePolicy()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = disk.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend setting the `disk_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages.")
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[1].longForm).isEqualTo("We recommend setting the `commit_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages.")
        assertThat(template).contains("Settings are inconsistent across the cluster")
        assertThat(template).contains("node1 = stop")
        assertThat(template).contains("node2 = die")
    }
}