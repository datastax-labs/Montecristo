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
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.storage.LsBlk
import com.datastax.montecristo.model.storage.Storage
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class TrickleFsyncTest {

    private val consistentTemplate = "## Trickle fsync\n" +
            "\n" +
            "The `trickle_fsync` can be used to provide better performance with SSDs by flushing buffers to disk more frequently, limiting the negative effects of flushing on latencies.\n" +
            "\n"+
            "`trickle_fsync` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "trickle_fsync: true \n" +
            "```\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentConsistent() {
        val configSetting = ConfigurationSetting("trickle_fsync", mapOf(Pair("node1", ConfigValue(true, "false","true")), Pair("node2", ConfigValue(true, "false","true"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("trickle_fsync", ConfigSource.CASS, "false") } returns configSetting

        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        val storage = mockk<Storage>(relaxed=true)
        every { storage.storageLocations.dataLocation() } returns "dm-0"
        every { storage.lsBlk.get("dm-0") } returns lsblk

        val cassandraYaml = mockk<CassandraYaml>(relaxed=true)
        every { cassandraYaml.get("trickle_fsync", "false", false)} returns "true"

        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml= cassandraYaml,  storage = storage, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml= cassandraYaml,  storage = storage, listenAddress = "10.0.0.2")
        every { cluster.nodes } returns listOf(node1, node2)


        val trickle = TrickleFsync()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = trickle.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(consistentTemplate)
    }

    @Test
    fun getDocumentMissingValue() {
        val configSetting = ConfigurationSetting("trickle_fsync", mapOf(Pair("node1", ConfigValue(false, "false","")), Pair("node2", ConfigValue(false, "false",""))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("trickle_fsync", ConfigSource.CASS, "false") } returns configSetting

        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        val storage = mockk<Storage>(relaxed=true)
        every { storage.storageLocations.dataLocation() } returns "dm-0"
        every { storage.lsBlk.get("dm-0") } returns lsblk

        val cassandraYaml = mockk<CassandraYaml>(relaxed=true)
        every { cassandraYaml.get("trickle_fsync", "false", false)} returns "false"

        val node = ObjectCreators.createNode(nodeName = "node1", cassandraYaml= cassandraYaml,  storage = storage, listenAddress = "10.0.0.1")
        every { cluster.nodes } returns listOf(node)

        val trickle = TrickleFsync()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = trickle.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).contains("false")
        assertThat(recs[0].longForm).isEqualTo("We recommend that all nodes with SSD / M2.NVMe storage should have the trickle_fsync value set to `true`")

    }
}