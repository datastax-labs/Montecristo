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

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.nodetool.Info
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Keyspace
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class ReplicationStrategyTest {

    @Test
    fun getDocumentNTSKeyspace() {

        val ks1 = Keyspace("test_keyspace_1", emptyList(), "org.apache.cassandra.locator.NetworkTopologyStrategy")
        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentSSKeyspace() {
        val ks1 = Keyspace("test_keyspace_1", emptyList(), "org.apache.cassandra.locator.SimpleStrategy")
        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend switching the following keyspaces to NetworkTopologyStrategy: test_keyspace_1")
    }

    @Test
    fun getDocumentOverReplicatedKeyspace() {
        val options: MutableList<Pair<String, Int>> = mutableListOf()
        options.add(Pair("dc1", 1))
        options.add(Pair("dc2", 4))

        val ks1 = Keyspace("test_keyspace_1", options, "org.apache.cassandra.locator.NetworkTopologyStrategy")

        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val nodeList: MutableList<Node> = mutableListOf()
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))

        every { cluster.nodes } returns nodeList

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend reducing the replication factor to be equal or less than the number of nodes for the following keyspaces: test_keyspace_1. Or increasing the number of nodes in the DC to match the replication factor.")
    }

    @Test
    fun getDocumentCorrectlyReplicatedKeyspace() {
        val options: MutableList<Pair<String, Int>> = mutableListOf()
        options.add(Pair("dc1", 3))
        options.add(Pair("dc2", 3))

        val ks1 = Keyspace("test_keyspace_1", options, "org.apache.cassandra.locator.NetworkTopologyStrategy")

        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val nodeList: MutableList<Node> = mutableListOf()
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))

        every { cluster.nodes } returns nodeList

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentSSAndOverReplicatedKeyspace() {
        val options: MutableList<Pair<String, Int>> = mutableListOf()
        options.add(Pair("replication_factor", 7))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        val ks1 = Keyspace("test_keyspace_1", options, "org.apache.cassandra.locator.SimpleStrategy")
        val keyspaces = listOf(ks1)
        every { cluster.schema.keyspaces } returns keyspaces

        val nodeList: MutableList<Node> = mutableListOf()
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))

        every { cluster.nodes } returns nodeList

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[1].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend switching the following keyspaces to NetworkTopologyStrategy: test_keyspace_1")
        assertThat(recs[1].longForm).isEqualTo("We recommend reducing the replication factor to be equal or less than the number of nodes for the following keyspaces: test_keyspace_1. Or increasing the number of nodes in the DC to match the replication factor.")
    }

    @Test
    fun getDocumentSSAndCorrectlyReplicatedKeyspace() {
        val options: MutableList<Pair<String, Int>> = mutableListOf()
        options.add(Pair("replication_factor", 5))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        val ks1 = Keyspace("test_keyspace_1", options, "org.apache.cassandra.locator.SimpleStrategy")
        val keyspaces = listOf(ks1)
        every { cluster.schema.keyspaces } returns keyspaces

        val nodeList: MutableList<Node> = mutableListOf()
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc1","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))
        nodeList.add(ObjectCreators.createNode(info = Info("dc2","")))

        every { cluster.nodes } returns nodeList

        val replication = ReplicationStrategy()
        val recs: MutableList<Recommendation> = mutableListOf()

        replication.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend switching the following keyspaces to NetworkTopologyStrategy: test_keyspace_1")
    }

}