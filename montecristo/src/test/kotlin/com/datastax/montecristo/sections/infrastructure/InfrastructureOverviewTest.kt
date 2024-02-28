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
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.Workload
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.application.DseYaml
import com.datastax.montecristo.model.metrics.Server
import com.datastax.montecristo.model.nodetool.*
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.*
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class InfrastructureOverviewTest() {

    @Test
    fun getDocumentNotAWS() {

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 16

        every { config.memInfo.memTotal } returns 33_554_432 // 32 GB in kb
        every { config.osReleaseName } returns "CentOS 7"
        val info1 = mockk<Info>(relaxed = true)
        every { info1.dataCenter } returns "DC1"
        every { info1.loadInBytes } returns 132070244352
        every { info1.rack } returns "a"
        val info2 = mockk<Info>(relaxed = true)
        every { info2.dataCenter } returns "DC1"
        every { info2.loadInBytes } returns 489626271744
        every { info2.rack } returns "b"
        val info3 = mockk<Info>(relaxed = true)
        every { info3.dataCenter } returns "DC1"
        every { info3.loadInBytes } returns 847182299136
        every { info3.rack } returns "c"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info1, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info2, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info3, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val server1 = Server("10.0.0.1", 32, "", "", true)
        val server2 = Server("10.0.0.2", 32, "", "", true)
        val server3 = Server("10.0.0.3", 32, "", "", true)

        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getServers() } returns listOf(server1, server2, server3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isAws } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.7")
        every { cluster.metricServer } returns metricServer

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val response = infra.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("node1|10.0.0.1|CentOS 7|DC1|a|CASSANDRA|16|31.25 GiB|132.07 GB|16")
        assertThat(response).contains("node2|10.0.0.2|CentOS 7|DC1|b|CASSANDRA|16|31.25 GiB|489.63 GB|16")
        assertThat(response).contains("node3|10.0.0.3|CentOS 7|DC1|c|CASSANDRA|16|31.25 GiB|847.18 GB|16")
        assertThat(response).doesNotContain("|||||||||")
    }

    @Test
    fun getDocumentAWSNodes() {

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 16

        every { config.memInfo.memTotal } returns 33_554_415 // 32 GB in kb
        val info1 = mockk<Info>(relaxed = true)
        every { info1.dataCenter } returns "DC1"
        every { info1.loadInBytes } returns 132070244352
        every { info1.rack } returns "a"
        val info2 = mockk<Info>(relaxed = true)
        every { info2.dataCenter } returns "DC1"
        every { info2.loadInBytes } returns 489626271744
        every { info2.rack } returns "b"
        val info3 = mockk<Info>(relaxed = true)
        every { info3.dataCenter } returns "DC1"
        every { info3.loadInBytes } returns 847182299136
        every { info3.rack } returns "c"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info1, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info2, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info3, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val server1 = Server("10.0.0.1", 32, "eu-west-1", "m4.xLarge", true)
        val server2 = Server("10.0.0.2", 32, "eu-west-1", "m4.xLarge", true)
        val server3 = Server("10.0.0.3", 32, "eu-west-1", "m4.xLarge", true)

        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getServers() } returns listOf(server1, server2, server3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isAws } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.7")
        every { cluster.metricServer } returns metricServer

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val response = infra.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("m4.xLarge|eu-west-1|3|0|0|null")
        assertThat(response).doesNotContain("|||||||||")
    }

    @Test
    fun getDocumentCheckIfNodesUsingMinimumRecommendedHardwareInsufficientCPU() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 4
        every { config.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns false

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val args = mutableMapOf<String, Any>()
        args["software"] = "Cassandra"

        infra.checkIfNodesUsingMinimumRecommendedHardware(cluster, recs, args)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend running Cassandra on hosts with at least 8 CPU cores. Cassandra uses a highly concurrent architecture with a large amount of parallelism. The hosts currently in use have only 4 CPU cores which is suboptimal for this architecture.")
    }

    @Test
    fun getDocumentCheckIfNodesUsingMinimumRecommendedHardwareTooMuchCPU() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 64
        every { config.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns false

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val args = mutableMapOf<String, Any>()
        args["software"] = "Cassandra"

        infra.checkIfNodesUsingMinimumRecommendedHardware(cluster, recs, args)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend running Cassandra on hosts with at most 32 cores. Cassandra uses a highly concurrent, staged event-driven architecture (SEDA) with a large amount of parallelism. As the core count increases the benefit per core diminishes.")
    }

    @Test
    fun getDocumentCheckIfNodesUsingMinimumRecommendedHardwareTooMuchCpuDSE() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 64
        every { config.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns true

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val args = mutableMapOf<String, Any>()
        args["software"] = "Cassandra"

        infra.checkIfNodesUsingMinimumRecommendedHardware(cluster, recs, args)
        assertThat(recs.size).isEqualTo(0)
      }

    @Test
    fun getDocumentCheckIfNodesUsingMinimumRecommendedHardwareInsufficientMemory() {

        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 16
        every { config.memInfo.memTotal } returns 20_000_000 // 32 GB in kb

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns true

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val args = mutableMapOf<String, Any>()
        args["software"] = "DSE"

        infra.checkIfNodesUsingMinimumRecommendedHardware(cluster, recs, args)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend running DSE on hosts with at least 32 GB of RAM, generally 64 GB or higher is a preferable in production.")
    }

    @Test
    fun getDocumentDifferentWorkloadType() {

        val info = mockk<Info>(relaxed = true)
        every { info.dataCenter } returns "DC1"
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info, listenAddress = "10.0.0.1", workload = listOf(Workload.CASSANDRA, Workload.SEARCH))
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info, listenAddress = "10.0.0.2", workload = listOf(Workload.CASSANDRA, Workload.ANALYTICS))
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info, listenAddress = "10.0.0.3", workload = listOf(Workload.CASSANDRA, Workload.GRAPH))
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkForMixedWorkloadDCs(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you do not run inconsistent workload types within a single DC, the following DCs have mixed workload types: DC1" )
    }

    @Test
    fun getDocumentAWSNodesInconsistentType() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val config = mockk<Configuration>(relaxed = true)
        every { config.lsCpu.getCpuThreads() } returns 16
        every { config.memInfo.memTotal } returns 33_554_415 // 32 GB in kb
        val info1 = mockk<Info>(relaxed = true)
        every { info1.dataCenter } returns "DC1"
        every { info1.loadInBytes } returns 132070244352
        every { info1.rack } returns "a"
        val info2 = mockk<Info>(relaxed = true)
        every { info2.dataCenter } returns "DC1"
        every { info2.loadInBytes } returns 489626271744
        every { info2.rack } returns "b"
        val info3 = mockk<Info>(relaxed = true)
        every { info3.dataCenter } returns "DC1"
        every { info3.loadInBytes } returns 847182299136
        every { info3.rack } returns "c"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info1, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.1")
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info2, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.2")
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info3, cassandraYaml = cassandraYaml, config = config, listenAddress = "10.0.0.3")
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val server1 = Server("10.0.0.1", 32, "eu-west-1", "m4.xLarge", true)
        val server2 = Server("10.0.0.2", 32, "eu-west-1", "m8.xLarge", true)
        val server3 = Server("10.0.0.3", 32, "eu-west-1", "m2.xLarge", true)

        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getServers() } returns listOf(server1, server2, server3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isAws } returns true
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.7")
        every { cluster.metricServer } returns metricServer
        every { cluster.getSetting("cluster_name", ConfigSource.CASS)} returns ConfigurationSetting("cluster_name", mapOf(Pair("node1", ConfigValue(true,"","foo"))))
        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        val response = infra.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("Your cluster is using different AWS instance types across nodes. We recommend using a single instance class/size to have a consistent performance throughout the cluster.")
        assertThat(response).contains("m4.xLarge|eu-west-1|1|0|0|null")
        assertThat(response).contains("m8.xLarge|eu-west-1|1|0|0|null")
        assertThat(response).contains("m2.xLarge|eu-west-1|1|0|0|null")
        assertThat(response).doesNotContain("|||||||||")
        assertThat(response).contains("foo")
    }

    @Test
    fun getDocumentCheckBadRackCount() {

        val infoRackA = mockk<Info>(relaxed = true)
        every { infoRackA.dataCenter } returns "DC1"
        every { infoRackA.rack } returns "a"
        val infoRackB = mockk<Info>(relaxed = true)
        every { infoRackB.dataCenter } returns "DC1"
        every { infoRackB.rack } returns "b"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = infoRackA)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = infoRackB)
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = infoRackB)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkRackCount(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you use either 1 or 3 racks within each DC. The following DC(s) are using alternative rack layouts: DC1. Racks may not be altered after a node has bootstrapped, changing the rack layout requires careful planning, please reach out to DataStax services if you need help with this activity.")
    }

    @Test
    fun getDocumentDSEFSEnabledWithAnalytics() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val dseYaml = mockk<DseYaml>(relaxed = true)
        every { dseYaml.get("dsefs_options.enabled", "false", false)  } returns "true"

        val node1 = ObjectCreators.createNode(nodeName = "node1", listenAddress = "10.0.0.1", isDse = true, workload = listOf(Workload.ANALYTICS), dseYaml = dseYaml)
        val node2 = ObjectCreators.createNode(nodeName = "node2", listenAddress = "10.0.0.2", isDse = true, workload = listOf(Workload.ANALYTICS), dseYaml = dseYaml)
        val node3 = ObjectCreators.createNode(nodeName = "node3", listenAddress = "10.0.0.3", isDse = true, workload = listOf(Workload.ANALYTICS), dseYaml = dseYaml)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns true

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfDSEFSEnabledIncorrectly(cluster, recs)
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentDSEFSEnabledWithNoAnalytics() {
        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 16
        val dseYaml = mockk<DseYaml>(relaxed = true)
        every { dseYaml.get("dsefs_options.enabled", "false", false)  } returns "true"

        val node1 = ObjectCreators.createNode(nodeName = "node1", listenAddress = "10.0.0.1", isDse = true, workload = listOf(Workload.CASSANDRA), dseYaml = dseYaml)
        val node2 = ObjectCreators.createNode(nodeName = "node2", listenAddress = "10.0.0.2", isDse = true, workload = listOf(Workload.CASSANDRA), dseYaml = dseYaml)
        val node3 = ObjectCreators.createNode(nodeName = "node3", listenAddress = "10.0.0.3", isDse = true, workload = listOf(Workload.CASSANDRA), dseYaml = dseYaml)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.isDse } returns true

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfDSEFSEnabledIncorrectly(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("DSE FS is enabled on 3 node(s), but there is no corresponding analytics workload. We recommend DSE FS is disabled on all nodes.")
    }

    @Test
    fun getDocumentNotEnoughNodes() {

        val info = mockk<Info>(relaxed = true)
        every { info.dataCenter } returns "DC1"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info)
        val nodeList: List<Node> = listOf(node1, node2)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList


        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfDCsHaveAtLeast3Nodes(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that each DC has a minimum of 3 nodes, the following DC(s) have less than 3 : DC1.")
    }

    @Test
    fun getDocumentNotEnoughNodesIn2DCs() {

        val info1 = mockk<Info>(relaxed = true)
        every { info1.dataCenter } returns "DC1"
        val info2 = mockk<Info>(relaxed = true)
        every { info2.dataCenter } returns "DC2"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info2)
        val nodeList: List<Node> = listOf(node1, node2)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfDCsHaveAtLeast3Nodes(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that each DC has a minimum of 3 nodes, the following DC(s) have less than 3 : DC1, DC2.")
    }

    @Test
    fun getDocument256TokenNodeV3() {

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 256

        val node1 = ObjectCreators.createNode(nodeName = "node1",  cassandraYaml = cassandraYaml)
        val node2 = ObjectCreators.createNode(nodeName = "node2",  cassandraYaml = cassandraYaml)
        val node3 = ObjectCreators.createNode(nodeName = "node3",  cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.10")
        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkVNodesValue(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].shortForm).contains("We recommend changing the number of vnodes to 16")
    }

    @Test
    fun getDocument256TokenNodeV2() {

        val cassandraYaml = mockk<CassandraYaml>(relaxed = true)
        every { cassandraYaml.vNodes } returns 256

        val node1 = ObjectCreators.createNode(nodeName = "node1",  cassandraYaml = cassandraYaml)
        val node2 = ObjectCreators.createNode(nodeName = "node2",  cassandraYaml = cassandraYaml)
        val node3 = ObjectCreators.createNode(nodeName = "node3",  cassandraYaml = cassandraYaml)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("2.1.0")
        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkVNodesValue(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].shortForm).contains("We recommend changing the number of vnodes to 64")
    }

    @Test
    fun getDocumentMismatchedHardwareInDC() {

        val config16cpu = mockk<Configuration>(relaxed = true)
        every { config16cpu.lsCpu.getCpuThreads() } returns 16
        every { config16cpu.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val config32cpu = mockk<Configuration>(relaxed = true)
        every { config32cpu.lsCpu.getCpuThreads() } returns 32
        every { config32cpu.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val info1 = mockk<Info>(relaxed = true)
        every { info1.dataCenter } returns "DC1"
        val info2 = mockk<Info>(relaxed = true)
        every { info2.dataCenter } returns "DC1"
        val info3 = mockk<Info>(relaxed = true)
        every { info3.dataCenter } returns "DC1"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info1, config = config16cpu)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info2, config = config32cpu)
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info3, config = config32cpu)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getDCNames() } returns listOf("DC1")
        every { cluster.nodes } returns nodeList
        every { cluster.getNodesFromDC("DC1")} returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfNodesHaveMismatchedHardware(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("Your cluster is using different hardware profiles (cpu / memory) within a single data center for the following data centers : DC1.")
    }

    @Test
    fun getDocumentMismatchedMemoryInDC() {

        val config38gbMem = mockk<Configuration>(relaxed = true)
        every { config38gbMem.lsCpu.getCpuThreads() } returns 32
        every { config38gbMem.memInfo.memTotal } returns 38_000_000 // more than 4gb higher
        every { config38gbMem.osReleaseName } returns "CentOS 7"

        val config31gbMem = mockk<Configuration>(relaxed = true)
        every { config31gbMem.lsCpu.getCpuThreads() } returns 32
        every { config31gbMem.memInfo.memTotal } returns 33_554_432 // 32 GB in kb
        every { config31gbMem.osReleaseName } returns "CentOS 7"

        val info = mockk<Info>(relaxed = true)
        every { info.dataCenter } returns "DC1"

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info, config = config38gbMem)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info, config = config31gbMem)
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info, config = config31gbMem)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getDCNames() } returns listOf("DC1")
        every { cluster.nodes } returns nodeList
        every { cluster.getNodesFromDC("DC1")} returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfNodesHaveMismatchedHardware(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("Your cluster is using different hardware profiles (cpu / memory) within a single data center for the following data centers : DC1.")
    }

    @Test
    fun getDocumentMismatchedHardwareInDifferentDC() {

        val config16cpu = mockk<Configuration>(relaxed = true)
        every { config16cpu.lsCpu.getCpuThreads() } returns 16
        every { config16cpu.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val config32cpu = mockk<Configuration>(relaxed = true)
        every { config32cpu.lsCpu.getCpuThreads() } returns 32
        every { config32cpu.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val infoDC1 = mockk<Info>(relaxed = true)
        every { infoDC1.dataCenter } returns "DC1"

        val infoDC2 = mockk<Info>(relaxed = true)
        every { infoDC2.dataCenter } returns "DC2"

        val node1 = ObjectCreators.createNode(nodeName = "node1",info = infoDC1,config = config16cpu)
        val node2 = ObjectCreators.createNode(nodeName = "node2",info = infoDC1,config = config16cpu)
        val node3 = ObjectCreators.createNode(nodeName = "node3",info = infoDC1,config = config16cpu)
        val node4 = ObjectCreators.createNode(nodeName = "node4",info = infoDC2,config = config32cpu)
        val node5 = ObjectCreators.createNode(nodeName = "node5",info = infoDC2,config = config32cpu)
        val node6 = ObjectCreators.createNode(nodeName = "node6",info = infoDC2,config = config32cpu)
        val nodeList: List<Node> = listOf(node1, node2, node3, node4, node5, node6)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getDCNames() } returns listOf("DC1", "DC2")
        every { cluster.nodes } returns nodeList
        every { cluster.getNodesFromDC("DC1") } returns listOf(node1, node2, node3)
        every { cluster.getNodesFromDC("DC2") } returns listOf(node4, node5, node6)

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfNodesHaveMismatchedHardware(cluster, recs)
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun testCheckIfTooMuchDataPerNode() {
        val config16cpu = mockk<Configuration>(relaxed = true)
        every { config16cpu.lsCpu.getCpuThreads() } returns 16
        every { config16cpu.memInfo.memTotal } returns 33_554_432 // 32 GB in kb

        val info3TB = mockk<Info>(relaxed = true)
        every { info3TB.loadInBytes } returns 3_000_000_000_000
        val info1TB = mockk<Info>(relaxed = true)
        every { info1TB.loadInBytes } returns 1_000_000_000_000

        val node1 = ObjectCreators.createNode(nodeName = "node1", info = info3TB, config = config16cpu)
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = info3TB, config = config16cpu)
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = info1TB, config = config16cpu)
        val nodeList: List<Node> = listOf(node1, node2, node3)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val infra = InfrastructureOverview()
        infra.checkIfTooMuchDataPerNode(cluster, recs)
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("2 nodes are storing more than 2TB of data each. We recommend that the number of nodes within the cluster is expanded to reduce the amount stored per node.")
    }
}
