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
import com.datastax.montecristo.fileLoaders.parsers.application.CassandraYamlParser
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.nodetool.Info
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test

internal class SeedsTest {

    private lateinit var dc1node1: Node
    private lateinit var dc1node2: Node
    private lateinit var dc1node3: Node
    private lateinit var dc1node4: Node
    private lateinit var dc2node1: Node
    private lateinit var dc2node2: Node
    private lateinit var dc2node3: Node
    private lateinit var dc2node4: Node
    private lateinit var dc1_3Nodes: List<Node>
    private lateinit var dc1_4Nodes: List<Node>
    private lateinit var dc2_3Nodes: List<Node>
    private lateinit var dc2_4Nodes: List<Node>

    private val consistentTemplate = "## Seeds\n" +
            "  \n" +
            "Seeds are used as \"well known\" nodes in the cluster. When a new node starts, it uses the seed list to find the other nodes in the cluster. They also function as \"super nodes\" in the Gossip protocol, where they receive gossip updates more frequently than other nodes. The list of seeds nodes is configured using the \"seeds\" configuration setting.  \n" +
            "  \n" +
            "The recommended approach is to use the same seed list for all nodes in a cluster and for it to contain three nodes from each DC.  \n" +
            "  \n" +
            "Seed list is configured as follows: \n" +
            "\n" +
            "```\n" +
            "- seeds: 10.0.0.1,10.0.0.2 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    private val inconsistentTemplate = "## Seeds\n" +
            "  \n" +
            "Seeds are used as \"well known\" nodes in the cluster. When a new node starts, it uses the seed list to find the other nodes in the cluster. They also function as \"super nodes\" in the Gossip protocol, where they receive gossip updates more frequently than other nodes. The list of seeds nodes is configured using the \"seeds\" configuration setting.  \n" +
            "  \n" +
            "The recommended approach is to use the same seed list for all nodes in a cluster and for it to contain three nodes from each DC.  \n" +
            "  \n" +
            "Seed list is inconsistent across the cluster: \n" +
            "\n" +
            "```\n" +
            "{\"seed_provider\"=[{\"class_name\"=\"org.apache.cassandra.locator.SimpleSeedProvider\",\"parameters\"=[{\"seeds\"=\"10.0.0.1,10.0.0.2\"}]}]} : 1 node\n" +
            "{\"seed_provider\"=[{\"class_name\"=\"org.apache.cassandra.locator.SimpleSeedProvider\",\"parameters\"=[{\"seeds\"=\"10.0.0.1,10.0.0.3\"}]}]} : 1 node\n" +
            "  \n" +
            "node1 = {\"seed_provider\"=[{\"class_name\"=\"org.apache.cassandra.locator.SimpleSeedProvider\",\"parameters\"=[{\"seeds\"=\"10.0.0.1,10.0.0.2\"}]}]}\n" +
            "node2 = {\"seed_provider\"=[{\"class_name\"=\"org.apache.cassandra.locator.SimpleSeedProvider\",\"parameters\"=[{\"seeds\"=\"10.0.0.1,10.0.0.3\"}]}]}\n" +
            "\n" +
            "```\n" +
            "\n" +
            "\n"

    private val enoughSeedsTemplate = "## Seeds\n" +
            "  \n" +
            "Seeds are used as \"well known\" nodes in the cluster. When a new node starts, it uses the seed list to find the other nodes in the cluster. They also function as \"super nodes\" in the Gossip protocol, where they receive gossip updates more frequently than other nodes. The list of seeds nodes is configured using the \"seeds\" configuration setting.  \n" +
            "  \n" +
            "The recommended approach is to use the same seed list for all nodes in a cluster and for it to contain three nodes from each DC.  \n" +
            "  \n" +
            "Seed list is configured as follows: \n" +
            "\n" +
            "```\n" +
            "- seeds: 10.0.0.1,10.0.0.2,10.0.0.3 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    @Before
    fun setUp() {
        dc1node1 = ObjectCreators.createNode(nodeName = "node1", info = Info("test_dc1", "", 0, 0), listenAddress = "10.0.0.1")
        dc1node2 = ObjectCreators.createNode(nodeName = "node2", info = Info("test_dc1", "", 0, 0), listenAddress = "10.0.0.2")
        dc1node3 = ObjectCreators.createNode(nodeName = "10.0.0.3", info = Info("test_dc1", "", 0, 0), listenAddress = "10.0.0.3")
        dc1node4 = ObjectCreators.createNode(nodeName = "node4", info = Info("test_dc1", "", 0, 0), listenAddress = "10.0.0.4")

        dc2node1 = ObjectCreators.createNode(nodeName = "10.0.0.5", info = Info("test_dc2", "", 0, 0), listenAddress = "10.0.0.5")
        dc2node2 = ObjectCreators.createNode(nodeName = "node2", info = Info("test_dc2", "", 0, 0), listenAddress = "10.0.0.6")
        dc2node3 = ObjectCreators.createNode(nodeName = "10.0.0.7", info = Info("test_dc2", "", 0, 0), listenAddress = "10.0.0.7")
        dc2node4 = ObjectCreators.createNode(nodeName = "node4", info = Info("test_dc2", "", 0, 0), listenAddress = "10.0.0.8")

        dc1_3Nodes = listOf(dc1node1, dc1node2, dc1node3)
        dc1_4Nodes = dc1_3Nodes.plus(dc1node4)
        dc2_3Nodes = listOf(dc2node2, dc2node2, dc2node3)
        dc2_4Nodes = dc2_3Nodes.plus(dc2node4)
    }

    @Test
    fun getDocumentSeedsConsistent() {

        val seedsTxt = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2"

        val cassandraYaml = CassandraYamlParser.parse(seedsTxt)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.2")
        val nodeList: List<Node> = listOf(node1, node2)

        val configSetting = mapOf(Pair("node1",  ConfigValue(true, "",cassandraYaml.data.toString())))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("seed_provider", ConfigSource.CASS).values } returns configSetting

        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend putting at least three nodes of each datacenter in the seeds list.")
        assertThat(template).isEqualTo(consistentTemplate)
    }

    @Test
    fun getDocumentSeedsConsistentWithDifferentSpacing() {

        val seedsTxt1 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1 ,10.0.0.2"

        val seedsTxt2 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2 "

        val cassandraYaml1 = CassandraYamlParser.parse(seedsTxt1)
        val cassandraYaml2 = CassandraYamlParser.parse(seedsTxt2)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2, listenAddress = "10.0.0.2")
        val nodeList: List<Node> = listOf(node1, node2)

        val configSetting = mapOf(Pair("node1",  ConfigValue(true, "",cassandraYaml1.data.toString())), Pair("node2",  ConfigValue(true, "",cassandraYaml2.data.toString())))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("seed_provider", ConfigSource.CASS).values } returns configSetting

        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend putting at least three nodes of each datacenter in the seeds list.")
    }

    @Test
    fun getDocumentSeedsConsistentWrongOrder() {

        val seedsTxt1 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2"

        val seedsTxt2 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.2,10.0.0.1"

        val cassandraYaml1 = CassandraYamlParser.parse(seedsTxt1)
        val cassandraYaml2 = CassandraYamlParser.parse(seedsTxt2)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2, listenAddress = "10.0.0.2")
        val nodeList: List<Node> = listOf(node1, node2)

        val configSetting = mapOf(Pair("node1",  ConfigValue(true, "",cassandraYaml1.data.toString())), Pair("node2",  ConfigValue(true, "",cassandraYaml2.data.toString())))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("seed_provider", ConfigSource.CASS).values } returns configSetting

        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend putting at least three nodes of each datacenter in the seeds list.")
        assertThat(template).isEqualTo(consistentTemplate)
    }

    @Test
    fun getDocumentSeedsInconsistent() {

        val seedsTxt1 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2"

        val seedsTxt2 = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.3"

        val cassandraYaml1 = CassandraYamlParser.parse(seedsTxt1)
        val cassandraYaml2 = CassandraYamlParser.parse(seedsTxt2)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2)
        val nodeList: List<Node> = listOf(node1, node2)

        val configSetting = mapOf(Pair("node1",  ConfigValue(true, "",cassandraYaml1.data.toString())), Pair("node2",  ConfigValue(true, "",cassandraYaml2.data.toString())))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("seed_provider", ConfigSource.CASS).values } returns configSetting
        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend using the same seed nodes across the cluster. Our advice is to use 3 nodes from each datacenter as seeds.")
        assertThat(template).isEqualTo(inconsistentTemplate)
    }

    @Test
    fun getDocumentEnoughSeeds() {

        val seedsTxt = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2,10.0.0.3"

        val cassandraYaml = CassandraYamlParser.parse(seedsTxt)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.3")
        val node4 = ObjectCreators.createNode(nodeName = "node4", cassandraYaml = cassandraYaml, listenAddress = "10.0.0.4")
        val nodeList: List<Node> = listOf(node1, node2, node3, node4)
        val jsonString = cassandraYaml.data.toString()
        val configSetting = mapOf(Pair("node1",  ConfigValue(true, "",jsonString)), Pair("node2",  ConfigValue(true, "",jsonString)), Pair("node3",  ConfigValue(true, "",jsonString)), Pair("node4",  ConfigValue(true, "",jsonString)))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.getSetting("seed_provider", ConfigSource.CASS).values } returns configSetting

        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0) // when there are enough, no recommendation will be issued
        assertThat(template).isEqualTo(enoughSeedsTemplate)
    }

    @Test
    fun testSingleDCSingleNodeSingleSeed() {
        assertThat(Seeds().hasEnoughSeeds(listOf(dc1node1), "10.0.0.1")).isFalse()
    }

    @Test
    fun testSingleDCThreeNodesSingleSeed() {
        assertThat(Seeds().hasEnoughSeeds(dc1_3Nodes, "10.0.0.1")).isFalse()
    }

    @Test
    fun testSingleDCThreeNodesThreeSeeds() {
        assertThat(Seeds().hasEnoughSeeds(dc1_3Nodes, "10.0.0.1,10.0.0.2,10.0.0.3")).isTrue()
    }

    @Test
    fun testSingleDCFourNodesThreeSeeds() {
        assertThat(Seeds().hasEnoughSeeds(dc1_4Nodes, "10.0.0.1,10.0.0.2,10.0.0.3")).isTrue()
    }

    @Test
    fun testTwoDCSixNodesSixSeedsBalanced() {
        assertThat(Seeds().hasEnoughSeeds(dc1_3Nodes.plus(dc2_3Nodes), "10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.5,10.0.0.6,10.0.0.7")).isTrue()
    }

    @Test
    fun testTwoDCEightNodesSixSeedsUnbalanced() {
        // the seeds are split 4/2 instead of 3/3, so it should consider there is not enough seeds
        assertThat(Seeds().hasEnoughSeeds(dc1_4Nodes.plus(dc2_4Nodes), "10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.4,10.0.0.4,10.0.0.6")).isFalse()
    }

    @Test
    fun testSeedConsistencyCheck() {
        // same text, simple happy path
        assertThat(Seeds().hasConsistentSeeds(listOf("10.0.0.1,10.0.0.2,10.0.0.3", "10.0.0.1,10.0.0.2,10.0.0.3"))).isTrue()
        // same seeds different order
        assertThat(Seeds().hasConsistentSeeds(listOf("10.0.0.1,10.0.0.3,10.0.0.2", "10.0.0.1,10.0.0.2,10.0.0.3"))).isTrue()
        // different seeds
        assertThat(Seeds().hasConsistentSeeds(listOf("10.0.0.1,10.0.0.4,10.0.0.2", "10.0.0.1,10.0.0.2,10.0.0.3"))).isFalse()
    }

    @Test
    fun getDocumentSeedNotInCluster() {

        val seedsTxt = "seed_provider:\n" +
                "- class_name: org.apache.cassandra.locator.SimpleSeedProvider\n" +
                "  parameters:\n" +
                "  - seeds: 10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.99"

        val cassandraYaml1 = CassandraYamlParser.parse(seedsTxt)
        val cassandraYaml2 = CassandraYamlParser.parse(seedsTxt)
        val cassandraYaml3 = CassandraYamlParser.parse(seedsTxt)
        val cassandraYaml4 = CassandraYamlParser.parse(seedsTxt)
        val node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", cassandraYaml = cassandraYaml3, listenAddress = "10.0.0.3")
        val node4 = ObjectCreators.createNode(nodeName = "node4", cassandraYaml = cassandraYaml4, listenAddress = "10.0.0.4")
        val nodeList: List<Node> = listOf(node1, node2, node3, node4)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val seeds = Seeds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = seeds.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(template).contains("10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.99")
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].shortForm).isEqualTo("We recommend checking the seed list to ensure that all of the seed IP addresses do belong to the cluster. A seed was found within the seed list which we could not confirm as being a member of the cluster.")

    }
}