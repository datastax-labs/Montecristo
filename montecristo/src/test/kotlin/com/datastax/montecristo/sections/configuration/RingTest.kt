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
import com.datastax.montecristo.fileLoaders.parsers.nodetool.RingParser
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Partitioner
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.withinPercentage
import org.junit.Before
import org.junit.Test

internal class RingTest {

    lateinit private var singleDcRing: List<com.datastax.montecristo.model.nodetool.Ring>
    lateinit private var singleDcSingleRing: List<com.datastax.montecristo.model.nodetool.Ring>
    lateinit private var multiDcRing: List<com.datastax.montecristo.model.nodetool.Ring>
    lateinit private var randomRing: List<com.datastax.montecristo.model.nodetool.Ring>

    private val templateSingleDC = "single_dc|10.100.100.31|29.10\n" +
            "single_dc|10.100.101.31|25.82\n" +
            "single_dc|10.100.100.30|25.05\n" +
            "single_dc|10.100.101.30|20.03\n"

    @Before
    fun setUp() {
        val singleDcSingleNodeFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/single_dc_singlenode_ring.txt").reader().readLines()
        singleDcSingleRing = RingParser.parse(singleDcSingleNodeFile)
        val singleDcRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/single_dc_ring.txt").reader().readLines()
        singleDcRing = RingParser.parse(singleDcRingFile)
        val multiDcRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/multi_dc_ring.txt").reader().readLines()
        multiDcRing = RingParser.parse(multiDcRingFile)
        val randomRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/randomPartitonerRing.txt").reader().readLines()
        randomRing = RingParser.parse(randomRingFile)
    }

    @Test
    fun testSingleDCSingleNodePercentageCalculation() {
        val results = Ring().calculateTokenPercentage(Partitioner.MURMUR, singleDcSingleRing)
        assertThat(results.containsKey("single_node_dc")).isTrue()
        assertThat(results.get("single_node_dc")?.size).isEqualTo(1) // there is only 1 node
        assertThat(results.get("single_node_dc")?.containsKey("172.18.0.11")).isTrue() // that the node IP was picked up
        assertThat(results.get("single_node_dc")?.get("172.18.0.11"))?.isCloseTo(100.0, withinPercentage(0.01)) // allow for precision problems, we want to be within 0.1% of the 100
    }

    @Test
    fun testSingleDCPercentageCalculation() {
        val results = Ring().calculateTokenPercentage(Partitioner.MURMUR, singleDcRing)
        assertThat(results.containsKey("single_dc")).isTrue()
        assertThat(results.get("single_dc")?.size).isEqualTo(4) // there are 4 nodes
        assertThat(results.get("single_dc")?.containsKey("10.100.100.30")).isTrue() // that the 4 node IPs were picked up
        assertThat(results.get("single_dc")?.containsKey("10.100.101.30")).isTrue()
        assertThat(results.get("single_dc")?.containsKey("10.100.100.31")).isTrue()
        assertThat(results.get("single_dc")?.containsKey("10.100.101.31")).isTrue()
        assertThat(results.get("single_dc")?.map { d -> d.value }?.sum()).isCloseTo(100.0, withinPercentage(0.01)) // all 4 nodes put together get to the 100
    }

    @Test
    fun testMultiDCPercentageCalculation() {
        val results = Ring().calculateTokenPercentage(Partitioner.MURMUR, multiDcRing)
        assertThat(results.size).isEqualTo(4) // 4 DCs were picked up
        assertThat(results.containsKey("first_dc")).isTrue()
        assertThat(results.containsKey("second_dc")).isTrue()
        assertThat(results.containsKey("third_dc")).isTrue()
        assertThat(results.containsKey("last_dc")).isTrue()
        assertThat(results.get("first_dc")?.size).isEqualTo(5)
        assertThat(results.get("second_dc")?.size).isEqualTo(5)
        assertThat(results.get("third_dc")?.size).isEqualTo(6)
        assertThat(results.get("last_dc")?.size).isEqualTo(3)

        assertThat(results.get("first_dc")?.map { d -> d.value }?.sum()).isCloseTo(100.0, withinPercentage(0.01)) // all 5 nodes put together get to the 100
        assertThat(results.get("second_dc")?.map { d -> d.value }?.sum()).isCloseTo(100.0, withinPercentage(0.01)) // all 5 nodes put together get to the 100
        assertThat(results.get("third_dc")?.map { d -> d.value }?.sum()).isCloseTo(100.0, withinPercentage(0.01)) // all 6 nodes put together get to the 100
        assertThat(results.get("last_dc")?.map { d -> d.value }?.sum()).isCloseTo(100.0, withinPercentage(0.01)) // all 3 nodes put together get to the 100
    }

    @Test
    fun testSingleDCSingleNodeBalanced() {
        val tokenResults = Ring().calculateTokenPercentage(Partitioner.MURMUR, singleDcSingleRing)
        print(tokenResults.toString())
        val balancedResults = Ring().isTokenRingBalanced(tokenResults, 0.25)
        assertThat(balancedResults.containsKey("single_node_dc")).isTrue()
        assertThat(balancedResults.get("single_node_dc")).isTrue()
    }

    @Test
    fun testSingleDCBalanced() {
        val tokenResults = Ring().calculateTokenPercentage(Partitioner.MURMUR, singleDcRing)
        print(tokenResults.toString())
        val balancedResults = Ring().isTokenRingBalanced(tokenResults, 0.25)
        assertThat(balancedResults.containsKey("single_dc")).isTrue()
        // single_dc
        // 10.100.100.30 = 25.05%
        // 10.100.101.31 = 25.81%
        // 10.100.100.31 = 29.10%
        // 10.100.101.30 = 20.03%
        // difference is significant enough from 29 to 20 to trigger is being shown as unbalanced,
        assertThat(balancedResults.get("single_dc")).isFalse()
    }


    @Test
    fun testSingleDCBalancedExtraTolerance() {
        val tokenResults = Ring().calculateTokenPercentage(Partitioner.MURMUR, singleDcRing)
        print(tokenResults.toString())
        // allowing a 40% difference
        val balancedResults = Ring().isTokenRingBalanced(tokenResults, 0.32)
        assertThat(balancedResults.containsKey("single_dc")).isTrue()
        // single_dc
        // 10.100.100.30 = 25.05%
        // 10.100.101.31 = 25.81%
        // 10.100.100.31 = 29.10%
        // 10.100.101.30 = 20.03%
        // difference is significant but with 32% tolerance, it is considered balanced (as long as the minimum is above 19.78%)
        assertThat(balancedResults.get("single_dc")).isTrue()
    }

    @Test
    fun testMultiDCBalanced() {
        val tokenResults = Ring().calculateTokenPercentage(Partitioner.MURMUR, multiDcRing)
        print(tokenResults.toString())
        val balancedResults = Ring().isTokenRingBalanced(tokenResults, 0.25)
        assertThat(balancedResults.containsKey("first_dc")).isTrue()
        assertThat(balancedResults.containsKey("second_dc")).isTrue()
        assertThat(balancedResults.containsKey("third_dc")).isTrue()
        assertThat(balancedResults.containsKey("last_dc")).isTrue()
        // first_dc
        // 10.115.99.104 = 18.75
        // 10.115.99.103 = 22.91
        // 10.115.99.106 = 18.87
        // 10.115.99.105 = 19.83
        // 10.115.99.107 = 19.65
        // max to min is inside the 25% tolerance
        assertThat(balancedResults.get("first_dc")).isTrue()
        // second_dc
        // 10.115.82.41 = 19.92%
        // 10.115.82.40 = 17.99%
        // 10.115.82.42 = 19.80%
        // 10.115.82.38 = 18.04%
        // 10.115.82.39 = 24.25%
        // max to min is outside tolerance, 24.25% * 0.75 = 18.18%, the 17.99% minimum is too low
        assertThat(balancedResults.get("second_dc")).isFalse()
        // third_dc
        // 10.115.66.73 = 16.75
        // 10.115.66.74 = 17.59
        // 10.115.66.77 = 18.19
        // 10.115.66.76 = 16.40
        // 10.115.66.69 = 16.62
        // 10.115.66.111 = 14.46
        // Max to min is inside the 25% tolerance.
        assertThat(balancedResults.get("third_dc")).isTrue()
        //last_dc
        // 10.115.66.109 = 29.35
        // 10.115.66.105 = 32.79
        // 10.115.66.100 = 37.86
        // Max to min is inside the 25% tolerance
        assertThat(balancedResults.get("last_dc")).isTrue()
    }


    @Test
    fun getDocumentCheckRecommendationAppears() {

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.partitioner } returns "org.apache.cassandra.dht.Murmur3Partitioner"
        val node1 = ObjectCreators.createNode(nodeName = "node1", ring = singleDcRing, cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val ring = Ring()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ring.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs.first().longForm).isEqualTo("The token range assignment is unbalanced in the following data centers : single_dc. Further work will need to be done to document the remediation process in a runbook. The process must be done by creating a new data center and shifting traffic to it.")
        assertThat(template).contains(templateSingleDC)
    }

    @Test
    fun testRandomRing2DCBalanced() {
        val tokenResults = Ring().calculateTokenPercentage(Partitioner.RANDOM, randomRing)
        print(tokenResults.toString())
        // DC1 & DC2
        // All Nodes were set manually with an initial token value and each has exactly 16.666% of the range, perfect balance.
        // we can use an incredibly small tolerance and it will still consider that it is balanced
        val balancedResults = Ring().isTokenRingBalanced(tokenResults, 0.01)
        assertThat(balancedResults["DC1"]).isTrue()
        assertThat(balancedResults["DC2"]).isTrue()
    }


}