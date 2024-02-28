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
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class NodeCommunicationsTest {

    private val consistentTemplate = "## Node Communications\n" +
            "\n" +
            "Nodes communicate with each other via two connections to every other node. The Command channel is used to send request messages, and the acknowledgement channel is used to send replies. Message passing between nodes is configured by several settings.\n" +
            "\n" +
            "The `inter_dc_tcp_nodelay` setting enables and disables the Nagle algorithm when communicating across data centers. By default `inter_dc_tcp_nodelay` is disabled, which enables the Nagle algorithm and reduces the amount of packets sent across the WAN. This is the correct setting to use when data centers are separated by a large lag, or unreliable networks such as the public internet.\n" +
            "\n" +
            "\n" +
            "`inter_dc_tcp_nodelay` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "inter_dc_tcp_nodelay: false \n" +
            "```\n" +
            "\n" +
            "\n"

    private val inconsistentTemplate = "## Node Communications\n" +
            "\n" +
            "Nodes communicate with each other via two connections to every other node. The Command channel is used to send request messages, and the acknowledgement channel is used to send replies. Message passing between nodes is configured by several settings.\n" +
            "\n" +
            "The `inter_dc_tcp_nodelay` setting enables and disables the Nagle algorithm when communicating across data centers. By default `inter_dc_tcp_nodelay` is disabled, which enables the Nagle algorithm and reduces the amount of packets sent across the WAN. This is the correct setting to use when data centers are separated by a large lag, or unreliable networks such as the public internet.\n" +
            "\n" +
            "\n" +
            "`inter_dc_tcp_nodelay` setting is inconsistent across the cluster: \n" +
            "\n" +
            "```\n" +
            "true : 1 node\n" +
            "false : 1 node\n" +
            "  \n" +
            "node1 = true\n" +
            "node2 = false\n" +
            "```\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentConsistent() {
        val configSetting = ConfigurationSetting("inter_dc_tcp_nodelay", mapOf(Pair("node1", ConfigValue(true, "false","false")), Pair("node2", ConfigValue(true, "false","false"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("inter_dc_tcp_nodelay", ConfigSource.CASS, "false") } returns configSetting
        every { cluster.isDse } returns false
        val tcp = NodeCommunications()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tcp.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(consistentTemplate)
    }

    @Test
    fun getDocumentInconsistent() {
        val configSetting = ConfigurationSetting("inter_dc_tcp_nodelay", mapOf(Pair("node1", ConfigValue(true, "false","true")), Pair("node2", ConfigValue(true, "false","false"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("inter_dc_tcp_nodelay", ConfigSource.CASS, "false") } returns configSetting
        every { cluster.isDse } returns false
        val tcp = NodeCommunications()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tcp.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(inconsistentTemplate)
    }
}