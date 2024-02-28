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
import com.datastax.montecristo.fileLoaders.parsers.os.NtpParser
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

internal class NtpTest {

    private val goodTemplateResult = "## NTP\n" +
            "\n" +
            "The NTP service ensures all nodes within the Cassandra ring always have their clocks synced to allow for reliable timestamp resolution.\n" +
            "\n" +
            "When using a driver for Cassandra that supports CQL 3.0+, the new default is to have client applications generate client-side timestamps to be used for mutation resolution. Because of this change in functionality, it is recommended to have NTP installed on all client nodes that issue mutation requests as well as all Cassandra nodes.\n" +
            "\n" +
            "\n" +
            "```\n" +
            "The ntp process is running everywhere.\n" +
            "\n" +
            "Recent NTP server names used are:\n" +
            "node1 *somewhere.isp.uk\n" +
            "node2 *somewhere.isp.uk\n" +
            "\n" +
            "Poll\tOffset\tJitter\tNode\n" +
            "1024\t-0.199\t0.434\tnode1\n" +
            "1024\t-0.199\t0.434\tnode2\n" +
            "\n" +
            "\n" +
            "```\n" +
            "\n" +
            "---\n" +
            "\n" +
            "_**Noted For Reference:** The NTP offset should be inside (-10, 10) ms interval._\n" +
            "\n" +
            "---\n"

    private val badTemplateResult = "## NTP\n" +
            "\n" +
            "The NTP service ensures all nodes within the Cassandra ring always have their clocks synced to allow for reliable timestamp resolution.\n" +
            "\n" +
            "When using a driver for Cassandra that supports CQL 3.0+, the new default is to have client applications generate client-side timestamps to be used for mutation resolution. Because of this change in functionality, it is recommended to have NTP installed on all client nodes that issue mutation requests as well as all Cassandra nodes.\n" +
            "\n" +
            "\n" +
            "```\n" +
            "The ntp process was NOT running:\n" +
            "node2\n" +
            "\n" +
            "Recent NTP server names used are:\n" +
            "node1 *somewhere.isp.uk\n" +
            "node2 *somewhere.isp.uk\n" +
            "\n" +
            "Poll\tOffset\tJitter\tNode\n" +
            "1024\t-0.199\t0.434\tnode1\n" +
            "1024\t-0.199\t0.434\tnode2\n" +
            "\n" +
            "\n" +
            "```\n" +
            "\n" +
            "---\n" +
            "\n" +
            "_**Noted For Reference:** The NTP offset should be inside (-10, 10) ms interval._\n" +
            "\n" +
            "---\n"

    private val ntpData = "     remote           refid      st t when poll reach   delay   offset  jitter\n" +
            "==============================================================================\n" +
            "*somewhere.isp.uk .LOCL.           1 u  979 1024  377    0.903   -0.199   0.434\n" +
            "+somewhere.isp.uk .LOCL.           1 u  543 1024  377    1.248   -0.084   0.135"

    private val ntpDataDifferentPoll = "     remote           refid      st t when poll reach   delay   offset  jitter\n" +
            "==============================================================================\n" +
            "*somewhere.isp.uk .LOCL.           1 u  979 1000  377    0.903   -0.199   0.434\n" +
            "+somewhere.isp.uk .LOCL.           1 u  543 1000  377    1.248   -0.084   0.135"

    private val psAuxRunning = "root      1459  0.0  0.0  26512  1952 ?        Ss   Jul24   8:05 /usr/lib/systemd/systemd-logind\n" +
            "dbus      1461  0.1  0.0  58312  2572 ?        Ss   Jul24 189:25 /usr/bin/dbus-daemon --system --address=systemd: --nofork --nopidfile --systemd-activation\n" +
            "ntp       1465  0.0  0.0  25728  1992 ?        Ss   Jul24   0:10 /usr/sbin/ntpd -u ntp:ntp -g\n" +
            "root      1472  0.0  0.0  21720  1404 ?        Ss   Jul24  32:42 /usr/sbin/irqbalance --foreground\n"

    private val psAuxNotRunning = "root      1459  0.0  0.0  26512  1952 ?        Ss   Jul24   8:05 /usr/lib/systemd/systemd-logind\n" +
            "dbus      1461  0.1  0.0  58312  2572 ?        Ss   Jul24 189:25 /usr/bin/dbus-daemon --system --address=systemd: --nofork --nopidfile --systemd-activation\n" +
            "root      1472  0.0  0.0  21720  1404 ?        Ss   Jul24  32:42 /usr/sbin/irqbalance --foreground\n"

    @Test
    fun getDocumentNtpRunning() {

        // we now need to construct a faux cluster with 2 nodes that will respond with the psAux above
        // and return jitter data
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.psAux.data } returns psAuxRunning.split("\n")
        every { config1.ntp} returns NtpParser.parse( ntpData.split ("\n"))
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.psAux.data } returns psAuxRunning.split("\n")
        every { config2.ntp} returns NtpParser.parse( ntpData.split ("\n"))

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1 )
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2 )
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val ntpSection = Ntp()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ntpSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(goodTemplateResult)
    }

    @Test
    fun getDocumentNtpNotAllRunning() {

        // we now need to construct a faux cluster with 2 nodes that will respond with
        // different psAuxes - 1 not present
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.psAux.data } returns psAuxRunning.split("\n")
        every { config1.ntp} returns NtpParser.parse( ntpData.split ("\n"))
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.psAux.data } returns psAuxNotRunning.split("\n")
        every { config2.ntp}returns NtpParser.parse( ntpData.split ("\n"))
        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1 )
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2 )
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val ntpSection = Ntp()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ntpSection.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // check the template, and that there is 1 recommendation
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs.first().longForm).isEqualTo("The NTP daemon (ntpd) was not detected running on all nodes in the cluster. Out of sync clocks in a Cassandra cluster can lead to inconsistencies that can be hard to diagnose. It is critical having ntpd running on all nodes in the cluster with monitoring and alerting of clock drift. We recommend verifying this.")
        assertThat(template).isEqualTo(badTemplateResult)
    }

    @Test
    fun getDocumentNtpRunningDifferentPolling() {

        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.psAux.data } returns psAuxRunning.split("\n")
        every { config1.ntp} returns NtpParser.parse( ntpData.split ("\n"))
        // 2nd node uses different ntp poll data so we trigger the check
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.psAux.data } returns psAuxRunning.split("\n")
        every { config2.ntp} returns NtpParser.parse( ntpDataDifferentPoll.split ("\n"))

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1 )
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2 )
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val ntpSection = Ntp()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ntpSection.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].shortForm).isEqualTo("We recommend using the same polling interval on every node.")
    }
}