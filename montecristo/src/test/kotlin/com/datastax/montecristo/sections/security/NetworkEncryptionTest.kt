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

package com.datastax.montecristo.sections.security

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class NetworkEncryptionTest {

    private val enabledTemplate = "## Network encryption\n" +
            "\n" +
            "Cassandra supports SSL encryption for both client-to-server and server-to-server communications. The server-to-server communications can be configured to encrypt cross Data Center messages only or all messages. Encryption is controlled by many configuration settings.  \n" +
            "\n" +
            "Server encryption is currently enabled : \n" +
            "\n" +
            "```\n" +
            "server_encryption_options: \n" +
            "    internode_encryption: enabled\n" +
            "```\n" +
            "\n" +
            "Client encryption is currently enabled : \n" +
            "\n" +
            "```\n" +
            "client_encryption_options: \n" +
            "    enabled: true\n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    private val notEnabledTemplate = "## Network encryption\n" +
            "\n" +
            "Cassandra supports SSL encryption for both client-to-server and server-to-server communications. The server-to-server communications can be configured to encrypt cross Data Center messages only or all messages. Encryption is controlled by many configuration settings.  \n" +
            "\n" +
            "Server encryption is currently disabled : \n" +
            "\n" +
            "```\n" +
            "server_encryption_options: \n" +
            "    internode_encryption: none\n" +
            "```\n" +
            "\n" +
            "Client encryption is currently disabled : \n" +
            "\n" +
            "```\n" +
            "client_encryption_options: \n" +
            "    enabled: false\n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    private val notConsistent = "## Network encryption\n" +
            "\n" +
            "Cassandra supports SSL encryption for both client-to-server and server-to-server communications. The server-to-server communications can be configured to encrypt cross Data Center messages only or all messages. Encryption is controlled by many configuration settings.  \n" +
            "\n" +
            "Server encryption is inconsistent across the cluster\n" +
            "\n" +
            "```\n" +
            "internode_encryption=none : 1 node\n" +
            "internode_encryption=Enabled : 1 node\n" +
            "node1 = none\n" +
            "node2 = Enabled\n" +
            "```\n" +
            "\n" +
            "Client encryption is inconsistent across the cluster\n" +
            "\n" +
            "```\n" +
            "enabled=false : 1 node\n" +
            "enabled=true : 1 node\n" +
            "\n" +
            "node1 = false\n" +
            "node2 = true\n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentEnabled() {

        val interNodeConfigSetting = ConfigurationSetting("internode_encryption", mapOf(Pair("node1",  ConfigValue(true, "none","enabled"))))
        val clientEncryptionConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1", ConfigValue(true, "false","true"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("server_encryption_options.internode_encryption", ConfigSource.CASS, "none") } returns interNodeConfigSetting
        every { cluster.getSetting("client_encryption_options.enabled", ConfigSource.CASS, "false") } returns clientEncryptionConfigSetting
        every { cluster.isDse } returns false
        val netEncryption = NetworkEncryption()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = netEncryption.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(enabledTemplate)
    }

    @Test
    fun getDocumentDisabled() {

        val interNodeConfigSetting = ConfigurationSetting("internode_encryption", mapOf(Pair("node1", ConfigValue(true, "none","none"))))
        val clientEncryptionConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(true, "false","false"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("server_encryption_options.internode_encryption", ConfigSource.CASS, "none") } returns interNodeConfigSetting
        every { cluster.getSetting("client_encryption_options.enabled", ConfigSource.CASS, "false") } returns clientEncryptionConfigSetting
        every { cluster.isDse } returns false
        val netEncryption = NetworkEncryption()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = netEncryption.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs[0].longForm).isEqualTo("We recommend implementing server-to-server encryption for all communications to enforce security. Further work will need to be done to document the process in a runbook as nodes using encryption and nodes with it disabled are unable to see one another.")
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs[1].longForm).isEqualTo("We recommend implementing client-to-server encryption to enforce security. Do note that passwords to connect to Cassandra are sent in plain text when encryption is turned off.")

        assertThat(template).isEqualTo(notEnabledTemplate)
    }

    @Test
    fun getDocumentNotConsistent() {

        val interNodeConfigSetting = ConfigurationSetting("internode_encryption", mapOf(Pair("node1", ConfigValue(true, "none","none")), Pair("node2", ConfigValue(true, "none","Enabled"))))
        val clientEncryptionConfigSetting = ConfigurationSetting("enabled", mapOf(Pair("node1",  ConfigValue(false, "false","")), Pair("node2",  ConfigValue(true, "false","true"))))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("server_encryption_options.internode_encryption", ConfigSource.CASS, "none") } returns interNodeConfigSetting
        every { cluster.getSetting("client_encryption_options.enabled", ConfigSource.CASS, "false") } returns clientEncryptionConfigSetting
        every { cluster.isDse } returns false
        val netEncryption = NetworkEncryption()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = netEncryption.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs[0].longForm).isEqualTo("We recommend implementing server-to-server encryption for all communications to enforce security. Further work will need to be done to document the process in a runbook as nodes using encryption and nodes with it disabled are unable to see one another.")
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs[1].longForm).isEqualTo("We recommend implementing client-to-server encryption to enforce security. Do note that passwords to connect to Cassandra are sent in plain text when encryption is turned off.")

        assertThat(template).isEqualTo(notConsistent)
    }

    @Test
    fun testIsDisabledSomewhere() {
        val settings1 = Pair("node1",  ConfigValue(true, "false","true"))
        val settings2 = Pair("node2",  ConfigValue(true, "false","true"))
        val settings3 = Pair("node3",  ConfigValue(true, "false","false"))

        val allEnabledSettings = mapOf(settings1, settings2)
        val serverToServerEncryption = ConfigurationSetting(
                name = "server_encryption_options",
                values = allEnabledSettings
        )
        assertFalse(
                NetworkEncryption().isDisabledSomewhere(serverToServerEncryption),
                "Network encryption was in fact enabled everywhere"
        )

        val oneDisabledSettings = mapOf(settings1, settings2, settings3)
        val clientToServerEncryption = ConfigurationSetting(
                name = "client_encryption_options",
                values = oneDisabledSettings
        )
        assertTrue(
                NetworkEncryption().isDisabledSomewhere(clientToServerEncryption),
                "Encryption options were disabled on some node"
        )
    }
}
