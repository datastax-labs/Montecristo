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
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.io.File

class ConfigurationMismatchesTest {

    lateinit var node1: Node
    lateinit var node2: Node
    lateinit var nodeNoFile: Node

    @Before
    fun setup() {
        val yamlFile1 = File(this.javaClass.getResource("/helpers/cassandra1.yaml").path)
        val cassandraYaml1 = CassandraYamlParser.parse(yamlFile1)
        val yamlFile2 = File(this.javaClass.getResource("/helpers/cassandra2.yaml").path)
        val cassandraYaml2 = CassandraYamlParser.parse(yamlFile2)
        // we now need to construct a node, we can fill most of it with mocks / test strings
        node1 = ObjectCreators.createNode(nodeName = "node1", cassandraYaml = cassandraYaml1)
        node2 = ObjectCreators.createNode(nodeName = "node2", cassandraYaml = cassandraYaml2)
        nodeNoFile = ObjectCreators.createNode(nodeName = "nodeNoFile")
    }

    @Test
    fun getConfigMismatchesWithNoDifference() {
        val nodeList = listOf<Node>(node1, node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getConfigMismatchesWithDifference() {
        val nodeList = listOf<Node>(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("1 differences")
        assertThat(recs[0].longForm).contains("1 missing settings")
        assertThat(recs[0].longForm).contains("0 additional settings")
    }

    @Test
    fun getConfigMismatchesWithNoDifference_MissingOneFile() {
        // This really tests two things: that there is a node with a missing config file, and that this works when that node is the first in the cluster list
        val nodeList = listOf<Node>(nodeNoFile,node1, node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every {nodeNoFile.cassandraYaml.isEmpty()} returns true

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("1 missing cassandra.yaml file")
    }

    @Test
    fun getConfigMismatchesWithDifference_MissingOneFile() {
        // This really tests two things: that there is a node with a missing config file, and that this works when that node is the first in the cluster list
        val nodeList = listOf<Node>(nodeNoFile,node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every {nodeNoFile.cassandraYaml.isEmpty()} returns true

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs.filter{it.longForm.contains("1 missing cassandra.yaml file")}.size).isEqualTo(1)
        assertThat(recs.filter{it.longForm.contains("1 differences")}.size).isEqualTo(1)
    }

    @Test
    fun getConfigMismatchesWithDifference_MissingAllFiles() {
        // This really tests two things: that there is a node with a missing config file, and that this works when that node is the first in the cluster list
        val nodeList = listOf<Node>(nodeNoFile,nodeNoFile,nodeNoFile)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every {nodeNoFile.cassandraYaml.isEmpty()} returns true

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("unable to compare configurations")
    }

    @Test
    fun getConfigMismatchesWithDifference_NoNodes() {
        // This really tests two things: that there is a node with a missing config file, and that this works when that node is the first in the cluster list
        val nodeList = listOf<Node>()
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val mismatches = ConfigurationMismatches()
        val recs: MutableList<Recommendation> = mutableListOf()

        mismatches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("unable to compare configurations")
    }

}