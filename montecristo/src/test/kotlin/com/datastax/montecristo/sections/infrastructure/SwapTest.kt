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

package com.datastax.montecristo.sections.infrastructure

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SwapTest {

    private val templateNoDifferences  = "## Swap\n" +
            "\n" +
            "Swap is used when RAM is full.  Generally speaking, Cassandra performs poorly when using swap space.  \n" +
            "\n" +
            "No swap files were detected on the nodes.\n" +
            "\n"

    private val templateDifferences = "## Swap\n" +
            "\n" +
            "Swap is used when RAM is full.  Generally speaking, Cassandra performs poorly when using swap space.  \n" +
            "\n" +
            "The nodes in this cluster are using different configuration settings.  This is a full breakdown of the configuration settings:\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Node</div></span>|<span><div style=\"text-align:left;\">Swap Partition Size</div></span>|<span><div style=\"text-align:left;\">vm.swappiness</div></span>\n" +
            "---|---|---\n" +
            "node1|200|1\n" +
            "node2|1000000|60\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentNoSwapNeeded() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 0
        every { config1.sysctl.vmswappiness } returns "1"
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 0
        every { config2.sysctl.vmswappiness } returns "1"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateNoDifferences)
    }

    @Test
    fun getDocumentSwapinessEnabled() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 0
        every { config1.sysctl.vmswappiness } returns "60"
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 0
        every { config2.sysctl.vmswappiness } returns "60"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs.first().longForm).isEqualTo("Swap space is set to zero but swappiness is set to 60.  We recommend setting vm.swappiness to 1 to prevent machines from accidentally using swap in the case of a configuration mistake in which a swap partition is created.")
        assertThat(template).isEqualTo(templateNoDifferences)
    }

    @Test
    fun getDocumentSwapNoSwappiness() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 1000000
        every { config1.sysctl.vmswappiness } returns "1"
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 1000000
        every { config2.sysctl.vmswappiness } returns "1"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(recs.first().longForm).isEqualTo("Swap space is enabled and set to 1000000, with swappiness disabled.  We recommend removing the swap partition.")
        assertThat(template).isEqualTo(templateNoDifferences)
    }

    @Test
    fun getDocumentSwapAndSwappiness() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 1000000
        every { config1.sysctl.vmswappiness } returns "60"
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 1000000
        every { config2.sysctl.vmswappiness } returns "60"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config1)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs.first().longForm).isEqualTo("Swap is enabled and set to 60, which can cause performance to dip when swap is used.  We recommend disabling swap by setting vm.swappiness = 1.")
        assertThat(template).isEqualTo(templateNoDifferences)
    }

    @Test
    fun getDocumentDifferentSettings() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 200
        every { config1.sysctl.vmswappiness } returns "1"
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 1000000
        every { config2.sysctl.vmswappiness } returns "60"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("Swap is enabled and set to 60, which can cause performance to dip when swap is used.  We recommend disabling swap by setting vm.swappiness = 1.")
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[1].longForm).isEqualTo("We recommend using uniform system settings, with swap disabled on all nodes in a cluster.")
        assertThat(template).isEqualTo(templateDifferences)
    }

    @Test
    fun getDocumentHandleNullVMSwap() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.memInfo.swapTotal } returns 0
        every { config1.sysctl.vmswappiness } returns null
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.memInfo.swapTotal } returns 0
        every { config2.sysctl.vmswappiness } returns null

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val swap = Swap()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = swap.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateNoDifferences)
    }
}