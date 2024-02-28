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

internal class AddressBindingsTest {

    private val addressSingleTemplate = "## Address Bindings\n" +
            "\n" +
            "Cassandra binds to two interfaces, and advertises three addresses to other nodes and clients. The two bound interfaces are used to communicate with other nodes and to communicate to clients. The third address that is advertised to other nodes is a \"Broadcast Address\" and is used when the node is behind a NAT. Communications are configured as follows:\n" +
            "\n" +
            "* Internode (node-to-node) is configured using the `listen_address`.\n" +
            "* Client (client-to-node) is configured using the `rpc_address`.\n" +
            "* Broadcast address is configured using the `broadcast_address`.\n" +
            "\n" +
            "The following settings are configured for address bindings : \n" +
            "\n" +
            "```\n" +
            "listen_address: 10.0.0.1 \n" +
            "listen_interface:  \n" +
            "\n" +
            "broadcast_address: 10.0.0.20 \n" +
            "broadcast_interface:  \n" +
            "\n" +
            "rpc_address: 10.0.0.10 \n" +
            "rpc_interface:  \n" +
            "\n" +
            "broadcast_rpc_address: 10.0.0.30 \n" +
            "broadcast_rpc_interface:  \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentSingleNodeAddresses() {

        val listenAddressConfigSetting = ConfigurationSetting("listen_address", mapOf(Pair("node1",
            ConfigValue(true, "","10.0.0.1"))
        ))
        val rpcAddressConfigSetting = ConfigurationSetting("rpc_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.10"))))
        val broadcastAddressConfigSetting = ConfigurationSetting("broadcast_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.20"))))
        val broadcastRpcAddressConfigSetting = ConfigurationSetting("broadcast_rpc_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.30"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("listen_address", ConfigSource.CASS) } returns listenAddressConfigSetting
        every { cluster.getSetting("rpc_address", ConfigSource.CASS) } returns rpcAddressConfigSetting
        every { cluster.getSetting("broadcast_address", ConfigSource.CASS) } returns broadcastAddressConfigSetting
        every { cluster.getSetting("broadcast_rpc_address", ConfigSource.CASS) } returns broadcastRpcAddressConfigSetting

        val address = com.datastax.montecristo.sections.configuration.AddressBindings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = address.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(addressSingleTemplate)
    }

    @Test
    fun getDocumentMultiNodeAddresses() {

        val listenAddressConfigSetting = ConfigurationSetting("listen_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.1")), Pair("node2",ConfigValue(true, "","10.0.0.100"))))
        val rpcAddressConfigSetting = ConfigurationSetting("rpc_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.10")), Pair("node2",ConfigValue(true, "","10.0.0.101"))))
        val broadcastAddressConfigSetting = ConfigurationSetting("broadcast_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.20")), Pair("node2",ConfigValue(true, "","10.0.0.102"))))
        val broadcastRpcAddressConfigSetting = ConfigurationSetting("broadcast_rpc_address", mapOf(Pair("node1",ConfigValue(true, "","10.0.0.30")), Pair("node2",ConfigValue(true, "","10.0.0.103"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("listen_address", ConfigSource.CASS) } returns listenAddressConfigSetting
        every { cluster.getSetting("rpc_address", ConfigSource.CASS) } returns rpcAddressConfigSetting
        every { cluster.getSetting("broadcast_address", ConfigSource.CASS) } returns broadcastAddressConfigSetting
        every { cluster.getSetting("broadcast_rpc_address", ConfigSource.CASS) } returns broadcastRpcAddressConfigSetting

        val address = com.datastax.montecristo.sections.configuration.AddressBindings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = address.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(addressSingleTemplate) // with multiple nodes, it still shows the first as an example
    }
}