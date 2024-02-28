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

package com.datastax.montecristo.fileLoaders.parsers.nodetool

import com.datastax.montecristo.model.Workload
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class GossipInfoParserTest {

    @Test
    fun testEmptyGossipInfoTxt() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/gossip/gossipinfo_empty.txt").reader().readLines()
        val gossipInfo = GossipInfoParser.parse (content)

        assertThat(gossipInfo.isEmpty()).isTrue
    }

    @Test
    fun testGossipParse() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/gossip/gossipinfo_oss.txt").reader().readLines()
        val gossipInfo = GossipInfoParser.parse (content)

        assertThat(gossipInfo.containsKey("10.51.41.59")).isTrue()
        val node1 = gossipInfo.get("10.51.41.59")
        assertThat(node1?.generation).isEqualTo(1612788628)
        assertThat(node1?.heartbeat).isEqualTo(4459098)
        assertThat(node1?.status).isEqualTo("NORMAL")
        assertThat(node1?.schema).isEqualTo("1c26302b-8e2a-3c80-a5dc-76fe0e493d4c")
        assertThat(node1?.dc).isEqualTo("DC2")
        assertThat(node1?.rack).isEqualTo("host04")
        assertThat(node1?.releaseVersion).isEqualTo("3.11.2")
        assertThat(node1?.internalIp).isEqualTo("10.51.41.59")
        assertThat(node1?.rpcAddress).isEqualTo("10.51.45.59")
        assertThat(node1?.hostId).isEqualTo("7c9af0b3-0ee7-49dc-8acb-d4a00e6ecd85")
        assertThat(node1?.rpcReady).isEqualTo(true)
        assertThat(node1?.getWorkloads()?.size).isEqualTo(1)
        assertThat(node1?.getWorkloads()?.first()).isEqualTo (Workload.CASSANDRA)

        assertThat(gossipInfo.containsKey("10.51.41.58")).isTrue()
        val node2 = gossipInfo.get("10.51.41.58")
        assertThat(node2?.generation).isEqualTo(1612789216)
        assertThat(node2?.heartbeat).isEqualTo(4458520)
        assertThat(node2?.status).isEqualTo("NORMAL")
        assertThat(node2?.schema).isEqualTo("1c26302b-8e2a-3c80-a5dc-76fe0e493d4c")
        assertThat(node2?.dc).isEqualTo("DC2")
        assertThat(node2?.rack).isEqualTo("host05")
        assertThat(node2?.releaseVersion).isEqualTo("3.11.2")
        assertThat(node2?.internalIp).isEqualTo("10.51.41.58")
        assertThat(node2?.rpcAddress).isEqualTo("10.51.45.58")
        assertThat(node2?.hostId).isEqualTo("4d89f1ca-6f3f-46da-9a68-0717db927143")
        assertThat(node2?.rpcReady).isEqualTo(true)
        assertThat(node2?.getWorkloads()?.size).isEqualTo(1)
        assertThat(node2?.getWorkloads()?.first()).isEqualTo (Workload.CASSANDRA)

        assertThat(gossipInfo.containsKey("10.51.41.57")).isTrue()
        val node3 = gossipInfo.get("10.51.41.57")
        assertThat(node3?.generation).isEqualTo(1612789168)
        assertThat(node3?.heartbeat).isEqualTo(4458490)
        assertThat(node3?.status).isEqualTo("NORMAL")
        assertThat(node3?.schema).isEqualTo("1c26302b-8e2a-3c80-a5dc-76fe0e493d4c")
        assertThat(node3?.dc).isEqualTo("DC2")
        assertThat(node3?.rack).isEqualTo("host05")
        assertThat(node3?.releaseVersion).isEqualTo("3.11.2")
        assertThat(node3?.internalIp).isEqualTo("10.51.41.57")
        assertThat(node3?.rpcAddress).isEqualTo("10.51.45.57")
        assertThat(node3?.hostId).isEqualTo("a85cbb17-238c-45f7-a20e-e91e9e264f4f")
        assertThat(node3?.rpcReady).isEqualTo(true)
        assertThat(node3?.getWorkloads()?.size).isEqualTo(1)
        assertThat(node3?.getWorkloads()?.first()).isEqualTo (Workload.CASSANDRA)

    }

    @Test
    fun testGossipDSEParse() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/gossip/gossipinfo_dse.txt").reader().readLines()
        val gossipInfo = GossipInfoParser.parse (content)

        assertThat(gossipInfo.containsKey("10.201.101.11")).isTrue()
        val node1 = gossipInfo.get("10.201.101.11")
        assertThat(node1?.generation).isEqualTo(1612419713)
        assertThat(node1?.heartbeat).isEqualTo(5988335)
        assertThat(node1?.status).isEqualTo("NORMAL")
        assertThat(node1?.schema).isEqualTo("fa06910e-28e5-3417-a381-096278de01f8")
        assertThat(node1?.dc).isEqualTo("AUSTIN")
        assertThat(node1?.rack).isEqualTo("rack1")
        assertThat(node1?.releaseVersion).isEqualTo("3.11.3.5117")
        assertThat(node1?.internalIp).isEqualTo("10.201.101.11")
        assertThat(node1?.rpcAddress).isEqualTo("10.201.104.11")
        assertThat(node1?.hostId).isEqualTo("5b5fc088-26a0-47a6-ac5a-de8071a2164d")
        assertThat(node1?.rpcReady).isEqualTo(true)
        assertThat(node1?.getWorkloads()?.size).isEqualTo(1)
        assertThat(node1?.getWorkloads()?.contains(Workload.CASSANDRA)).isTrue()

        assertThat(gossipInfo.containsKey("10.201.101.13")).isTrue()
        val node2 = gossipInfo.get("10.201.101.13")
        assertThat(node2?.generation).isEqualTo(1574682326)
        assertThat(node2?.heartbeat).isEqualTo(46224298)
        assertThat(node2?.status).isEqualTo("NORMAL")
        assertThat(node2?.schema).isEqualTo("fa06910e-28e5-3417-a381-096278de01f8")
        assertThat(node2?.dc).isEqualTo("AUSTIN")
        assertThat(node2?.rack).isEqualTo("rack1")
        assertThat(node2?.releaseVersion).isEqualTo("3.11.3.5117")
        assertThat(node2?.internalIp).isEqualTo("10.201.101.13")
        assertThat(node2?.rpcAddress).isEqualTo("10.201.104.13")
        assertThat(node2?.hostId).isEqualTo("96bf836c-8dd8-4eab-a6e5-abfb3e44287d")
        assertThat(node2?.rpcReady).isEqualTo(true)
        assertThat(node2?.getWorkloads()?.size).isEqualTo(2)
        assertThat(node2?.getWorkloads()?.contains(Workload.CASSANDRA)).isTrue()
        assertThat(node2?.getWorkloads()?.contains(Workload.ANALYTICS)).isTrue()

        assertThat(gossipInfo.containsKey("10.201.101.12")).isTrue()
        val node3 = gossipInfo.get("10.201.101.12")
        assertThat(node3?.generation).isEqualTo(1612421321)
        assertThat(node3?.heartbeat).isEqualTo(5986624)
        assertThat(node3?.status).isEqualTo("NORMAL")
        assertThat(node3?.schema).isEqualTo("fa06910e-28e5-3417-a381-096278de01f8")
        assertThat(node3?.dc).isEqualTo("AUSTIN")
        assertThat(node3?.rack).isEqualTo("rack1")
        assertThat(node3?.releaseVersion).isEqualTo("3.11.3.5117")
        assertThat(node3?.internalIp).isEqualTo("10.201.101.12")
        assertThat(node3?.rpcAddress).isEqualTo("10.201.104.12")
        assertThat(node3?.hostId).isEqualTo("d8ff6948-d66b-4d26-9b33-179968ebf60b")
        assertThat(node3?.rpcReady).isEqualTo(true)
        assertThat(node3?.getWorkloads()?.size).isEqualTo(2)
        assertThat(node3?.getWorkloads()?.contains(Workload.CASSANDRA)).isTrue()
        assertThat(node3?.getWorkloads()?.contains(Workload.GRAPH)).isTrue()
    }

    @Test
    fun testGossipDSEAlternativeParse() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/gossip/gossip_info_dse_alternative.txt").reader().readLines()
        val gossipInfo = GossipInfoParser.parse (content)

        assertThat(gossipInfo.containsKey("10.163.71.160")).isTrue()
        val node1 = gossipInfo.get("10.163.71.160")
        assertThat(node1?.generation).isEqualTo(1620340026)
        assertThat(node1?.heartbeat).isEqualTo(444554)
        assertThat(node1?.status).isEqualTo("NORMAL")
        assertThat(node1?.schema).isEqualTo("5fb19da1-46d2-36b3-a454-6cef17c9dd83")
        assertThat(node1?.dc).isEqualTo("Cassandra")
        assertThat(node1?.rack).isEqualTo("rack3")
        assertThat(node1?.releaseVersion).isEqualTo("4.0.0.677")
        assertThat(node1?.internalIp).isEqualTo("10.163.71.160")
        assertThat(node1?.hostId).isEqualTo("64449de7-40a2-4fc5-94e3-1ddbad6928fc")
        assertThat(node1?.rpcReady).isEqualTo(false)
        assertThat(node1?.getWorkloads()?.size).isEqualTo(4)
        assertThat(node1?.getWorkloads()?.contains(Workload.CASSANDRA)).isTrue()
        assertThat(node1?.getWorkloads()?.contains(Workload.SEARCH)).isTrue()
        assertThat(node1?.getWorkloads()?.contains(Workload.GRAPH)).isTrue()
        assertThat(node1?.getWorkloads()?.contains(Workload.ANALYTICS)).isTrue()
    }
}

