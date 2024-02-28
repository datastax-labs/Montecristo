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
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class OSConfigOverviewTest {

    @Test
    fun getDocumentNoIssues() {

        // we now need to construct a faux cluster with 2 nodes that will respond with the psAux above
        // and return jitter data
        val config = mockk<Configuration>(relaxed = true)
        every { config.psAux.getCassandraRunningUser() } returns "cassandra"
        every { config.limits.getMemLock("cassandra") } returns "unlimited"
        every { config.limits.getNoFile("cassandra") } returns "100000"
        every { config.limits.getNProc("cassandra") } returns "32768"
        every { config.limits.getAddressSetting("cassandra") } returns "unlimited"

        every { config.sysctl.vmMaxMapCount } returns "1048575"
        every { config.sysctl.netRmemMax } returns "16777216"
        every { config.sysctl.netWmemMax } returns "16777216"
        every { config.sysctl.netRmemDefault } returns "16777216"
        every { config.sysctl.netWmemDefault } returns "16777216"
        every { config.sysctl.netOptMemMax } returns "40960"
        every { config.sysctl.netIpv4TcpRmem } returns "4096 87380 16777216"
        every { config.sysctl.netIpv4TcpWmem } returns "4096 65536 16777216"

        every { config.hugePages.isSetNever() } returns true

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config )
        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val osConfigSection = OSConfigOverview()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = osConfigSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentFailEverything() {

        // we now need to construct a faux cluster with 2 nodes that will respond with the psAux above
        // and return jitter data
        val config = mockk<Configuration>(relaxed = true)
        every { config.psAux.getCassandraRunningUser() } returns "cassandra"
        every { config.limits.getMemLock("cassandra") } returns "limited"
        every { config.limits.getNoFile("cassandra") } returns "100"
        every { config.limits.getNProc("cassandra") } returns "32"
        every { config.limits.getAddressSetting("cassandra") } returns "limited"

        every { config.sysctl.vmMaxMapCount } returns "10"
        every { config.sysctl.netRmemMax } returns "16"
        every { config.sysctl.netWmemMax } returns "16"
        every { config.sysctl.netRmemDefault } returns "16"
        every { config.sysctl.netWmemDefault } returns "16"
        every { config.sysctl.netOptMemMax } returns "40"
        every { config.sysctl.netIpv4TcpRmem } returns "1 1 1"
        every { config.sysctl.netIpv4TcpWmem } returns "1 1 1"

        every { config.hugePages.isSetNever() } returns false

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config )
        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val osConfigSection = OSConfigOverview()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = osConfigSection.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        // number of failures
        assertThat(template).contains("All Nodes")
        assertThat(recs[0].longForm).contains("13")
    }
}