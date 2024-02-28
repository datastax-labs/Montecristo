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
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.BufferedReader
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CustomSettingsTest {

    private val customYamlCompareTemplate = "## Custom settings\n" +
            "  \n" +
            "Below are all the settings from the `cassandra.yaml` file that were customized:  \n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Setting</div></span>|<span><div style=\"text-align:left;\">Base</div></span>|<span><div style=\"text-align:left;\">Current</div></span>\n" +
            "---|---|---\n" +
            "compaction_throughput_mb_per_sec|16|32\n" +
            "num_tokens|256|64\n" +
            "\n" +
            "\n"

    private val customYamlBlankSettingCompareTemplate = "## Custom settings\n" +
            "  \n" +
            "Below are all the settings from the `cassandra.yaml` file that were customized:  \n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Setting</div></span>|<span><div style=\"text-align:left;\">Base</div></span>|<span><div style=\"text-align:left;\">Current</div></span>\n" +
            "---|---|---\n" +
            "compaction_throughput_mb_per_sec|16|32\n" +
            "num_tokens|256|\n" +
            "\n" +
            "\n"

    @Test
    fun testDownloadCassandraYaml() {
        val cassandraYaml = CustomSettings.downloadBaseConfiguration(false, DatabaseVersion.fromString ("3.11.3"))
        assertTrue(cassandraYaml.contains("num_tokens"))
        assertTrue(cassandraYaml.contains("cluster_name"))
    }

    @Test
    fun parseDownloadedCassandraYaml() {
        val cassandraYaml = CustomSettings.downloadBaseConfiguration(false, DatabaseVersion.fromString ( "3.11.6"))
        val parsedConfig = CassandraYamlParser.parse(cassandraYaml)
        assertEquals(256, parsedConfig.vNodes)
        assertTrue(!parsedConfig.data.path("cluster_name").isMissingNode)
    }

    @Test
    fun parseDownloadedCassandraYamlDSE() {
        val cassandraYaml = CustomSettings.downloadBaseConfiguration(true, DatabaseVersion.fromString ( "6.7.6", true))
        val parsedConfig = CassandraYamlParser.parse(cassandraYaml)
        assertEquals("com.datastax.bdp.cassandra.auth.DseAuthorizer", parsedConfig.authorizer)
        assertTrue(!parsedConfig.data.path("cluster_name").isMissingNode)
    }

    @Test
    fun parseDownloadedCassandraYamlDSE5_1_99() {
        val cassandraYaml = CustomSettings.downloadBaseConfiguration(true, DatabaseVersion.fromString ( "5.1.99", true))
        val parsedConfig = CassandraYamlParser.parse(cassandraYaml)
        assertEquals("com.datastax.bdp.cassandra.auth.DseAuthorizer", parsedConfig.authorizer)
        assertTrue(!parsedConfig.data.path("cluster_name").isMissingNode)
    }

    @Test
    fun compareCustomYamlWithBaseVersion() {
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3.yaml")
        val reader = BufferedReader(customCassandraYaml.reader())
        val customYamlContent: String = reader.use {
            it.readText()
        }

        val custom = CassandraYamlParser.parse(customYamlContent).asMap()
        val base = CassandraYamlParser.parse(CustomSettings.downloadBaseConfiguration(false, DatabaseVersion.fromString("3.11.3"))).asMap()
        val differences = CustomSettings.isolateCustomSettings(base, custom)
        assertFalse(differences.isEmpty())
        assertTrue(differences.containsKey("num_tokens"))
        assertTrue(differences.containsKey("compaction_throughput_mb_per_sec"))
        assertFalse(differences.containsKey("column_index_cache_size_in_kb"))
        assertEquals(16, differences["compaction_throughput_mb_per_sec"]!!.first)
        assertEquals(32, differences["compaction_throughput_mb_per_sec"]!!.second)
        assertEquals(256, differences["num_tokens"]!!.first)
        assertEquals(64, differences["num_tokens"]!!.second)

    }

    @Test
    fun compareCustomYamlWithBaseVersionDocument() {
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3.yaml")
        val cassandraYaml = CassandraYamlParser.parse(customCassandraYaml.reader().readText())

        val node = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val settings = CustomSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = settings.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(customYamlCompareTemplate)
    }

    @Test
    fun compareCustomYamlWithBaseVersionSettingRemoved() {
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3.yaml")
        val reader = BufferedReader(customCassandraYaml.reader())
        val customYamlContent: String = reader.use {
            it.readText()
        }
        // strip out the num tokens so we have no value specified.
        val strippedContent = customYamlContent.replace("num_tokens: 64", "num_tokens:")
        val cassandraYaml = CassandraYamlParser.parse(strippedContent)

        val node = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val settings = CustomSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = settings.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(customYamlBlankSettingCompareTemplate)
    }

    @Test
    fun customSettingsNotInBaseYaml() {

        // get the custom yaml
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3.yaml")
        val reader = BufferedReader(customCassandraYaml.reader())
        val customYamlContent: String = reader.use {
            it.readText()
        }
        val custom: MutableMap<String, Any> = CassandraYamlParser.parse(customYamlContent).asMap().toMutableMap()

        // add a setting that's not in the base yaml
        // the default is 86400000
        custom["streaming_socket_timeout_in_ms"] = 36000000

        // get the base yaml
        val base = CassandraYamlParser.parse(CustomSettings.downloadBaseConfiguration(false, DatabaseVersion.latest311())).asMap()

        val differences = CustomSettings.isolateCustomSettings(base, custom.toMap())
        assertTrue(
                differences.containsKey("streaming_socket_timeout_in_ms"),
                "A setting that is not present in the base yaml, but was customised, did not make it to the differences"
        )
        assertEquals(
                Pair("blank", 36000000),
                differences["streaming_socket_timeout_in_ms"],
                "A setting present in a base yaml only had a wrong difference found"
        )
    }

    @Test
    fun removePasswordFieldsTest() {
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3.yaml")
        val cassandraYaml = CassandraYamlParser.parse(customCassandraYaml.reader().readText())

        assertThat(cassandraYaml.get("server_encryption_options.internode_encryption")).isNotEqualTo("")
        assertThat(cassandraYaml.get("server_encryption_options.keystore_password")).isNotEqualTo("")
        assertThat(cassandraYaml.get("server_encryption_options.truststore_password")).isNotEqualTo("")

        val yamlWithoutPasswords = CustomSettings.removePasswordFields(cassandraYaml.asMap())

        assertThat(yamlWithoutPasswords.size).isEqualTo(92)
        @Suppress("UNCHECKED_CAST")
        assertTrue((yamlWithoutPasswords.get("server_encryption_options") as HashMap<String,String>).keys.contains("internode_encryption"))
        @Suppress("UNCHECKED_CAST")
        assertFalse((yamlWithoutPasswords.get("server_encryption_options") as HashMap<String,String>).keys.contains("keystore_password"))
    }

    @Test
    fun checkMultiDirectoryRecommendation() {
        val customCassandraYaml = this.javaClass.getResourceAsStream("/cassandra-3.11.3-multi-directory.yaml")
        val cassandraYaml = CassandraYamlParser.parse(customCassandraYaml.reader().readText())
        assertThat(cassandraYaml.get("data_file_directories","",true)).isEqualTo("/device1/folder1/cassandra/data,/device2/folder2/cassandra/data")

        val node = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val settings = CustomSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = settings.getDocument(cluster, searcher, recs, ExecutionProfile.default())

        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you provision sufficient space on a single volume so that only a single data file directory is configured. The use of multiple data directories has significant disadvantages.")
    }


}