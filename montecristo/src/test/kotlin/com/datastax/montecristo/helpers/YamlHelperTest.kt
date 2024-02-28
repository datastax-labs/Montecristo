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

package com.datastax.montecristo.helpers

import com.datastax.montecristo.fileLoaders.parsers.application.CassandraYamlParser
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.io.File

internal class YamlHelperTest {

    lateinit var node1: Node
    lateinit var node2: Node

    @Before
    fun setup() {
        val yamlFile1 = File(this.javaClass.getResource("/helpers/cassandra1.yaml").path)
        val cassandraYaml1 = CassandraYamlParser.parse(yamlFile1)
        val yamlFile2 = File(this.javaClass.getResource("/helpers/cassandra2.yaml").path)
        val cassandraYaml2 = CassandraYamlParser.parse(yamlFile2)
        // we now need to construct a node, we can fill most of it with mocks / test strings
        node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1)
        node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2)
    }

    @Test
    fun getConfigMismatchesWithNoDifference() {
        val nodeList = listOf<Node>(node1, node1)
        val differences = ConfigHelper.getConfigMismatches(nodeList, ConfigSource.CASS)
        assertThat(differences.first).isEqualTo(node1.hostname)
        assertThat(differences.second.size).isEqualTo(0)
    }

    @Test
    fun getConfigMismatchesWithDifference() {
        val nodeList = listOf<Node>(node1, node2)
        val differences = ConfigHelper.getConfigMismatches(nodeList, ConfigSource.CASS)
        assertThat(differences.second.size).isEqualTo(1)
        assertThat(differences.second.get("node2")?.entriesDiffering()?.get("num_tokens")?.leftValue()).isEqualTo(256)
        assertThat(differences.second.get("node2")?.entriesDiffering()?.get("num_tokens")?.rightValue()).isEqualTo(128)
    }

    @Test
    fun getConfigMissingKeys() {
        val nodeList = listOf<Node>(node1, node2)
        val differences = ConfigHelper.getConfigMissingKeys(nodeList, ConfigSource.CASS)
        assertThat(differences.second.get("node2")?.get("Missing")?.size).isEqualTo(1)
        assertThat(differences.second.get("node2")?.get("Additional")?.size).isEqualTo(0)
        assertThat(differences.second.get("node2")?.get("Missing")?.contains("extra_key")).isEqualTo(true)
    }

    @Test
    fun getConfigSettingDifferent() {
        val configSetting = ConfigurationSetting("num_tokens", mapOf(Pair("node1", ConfigValue(true, "","8")), Pair("node2", ConfigValue(true, "","128"))))
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("num_tokens", ConfigSource.CASS) } returns configSetting

        val numTokens = cluster.getSetting("num_tokens", ConfigSource.CASS)
        assertThat(numTokens.isConsistent()).isFalse()
        assertThat(numTokens.getDistinctValues().size).isEqualTo(2)
    }

    @Test
    fun getConfigSettingSame() {
        val configSetting = ConfigurationSetting("cluster_name", mapOf(Pair("node1", ConfigValue(true, "","test")), Pair("node2", ConfigValue(true, "","test"))))
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("cluster_name", ConfigSource.CASS) } returns configSetting

        val numTokens = cluster.getSetting("cluster_name", ConfigSource.CASS)
        assertThat(numTokens.isConsistent()).isTrue()
        assertThat(numTokens.getDistinctValues().size).isEqualTo(1)
    }
}
