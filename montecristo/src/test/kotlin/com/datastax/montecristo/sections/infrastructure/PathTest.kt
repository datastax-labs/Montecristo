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

internal class PathTest {

    private val env  = "MANPATH=:/opt/puppetlabs/puppet/share/man\n" +
            "XDG_SESSION_ID=55765\n" +
            "SHELL=/bin/bash\n" +
            "SSH_CLIENT=10.115.122.15 45450 22\n" +
            "USER=root\n" +
            "MIBS=+LOGATE-MIB\n" +
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/opt/puppetlabs/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "MAIL=/var/mail/root\n" +
            "PWD=/root\n" +
            "LANG=en_US.UTF-8\n" +
            "HOME=/root\n" +
            "SHLVL=2\n" +
            "LOGNAME=root\n" +
            "SSH_CONNECTION=10.115.122.15 45450 10.115.122.16 22\n" +
            "LESSOPEN=||/usr/bin/lesspipe.sh %s\n" +
            "XDG_RUNTIME_DIR=/run/user/0\n" +
            "_=/usr/bin/env\n"

    private val envPathChanged  = "MANPATH=:/opt/puppetlabs/puppet/share/man\n" +
            "XDG_SESSION_ID=55765\n" +
            "SHELL=/bin/bash\n" +
            "SSH_CLIENT=10.115.122.15 45450 22\n" +
            "USER=root\n" +
            "MIBS=+LOGATE-MIB\n" +
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "MAIL=/var/mail/root\n" +
            "PWD=/root\n" +
            "LANG=en_US.UTF-8\n" +
            "HOME=/root\n" +
            "SHLVL=2\n" +
            "LOGNAME=root\n" +
            "SSH_CONNECTION=10.115.122.15 45450 10.115.122.16 22\n" +
            "LESSOPEN=||/usr/bin/lesspipe.sh %s\n" +
            "XDG_RUNTIME_DIR=/run/user/0\n" +
            "_=/usr/bin/env\n"

    private val templateNoDifferences = "## Path\n" +
            "\n" +
            "The `\$PATH` environment variable has the following configuration:\n" +
            "\n" +
            "```\n" +
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/opt/puppetlabs/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "```\n" +
            "\n" +
            "All nodes have the `\$PATH` variable configured in the same way.\n" +
            "\n" +
            "\n"

    private val templateDifferences = "## Path\n" +
            "\n" +
            "The `\$PATH` environment variable has the following configuration:\n" +
            "\n" +
            "```\n" +
            "Count\tPath\n" +
            "1\tPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/opt/puppetlabs/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "1\tPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "\n" +
            "Path: PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/opt/puppetlabs/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "Nodes: [node1]\n" +
            "\n" +
            "Path: PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin:/sbin\n" +
            "Nodes: [node2]\n" +
            "\n" +
            "\n" +
            "```\n" +
            "\n" +
            "There are differences in the configuration of the `\$PATH` variable.\n" +
            "\n" +
            "We recommend configuring the PATH variable uniformly on each node.\n" +
            "\n"

    @Test
    fun getDocumentConsistentPath() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.env.data } returns env.split("\n")
        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.env.data } returns env.split("\n")

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val path = Path()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = path.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateNoDifferences)
    }

    @Test
    fun getDocumentInconsistentPath() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.env.data } returns env.split("\n")
        every { config1.env.path} returns "extracted/node1_artifacts_2020_10_06_1140_1601977214"

        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.env.data } returns envPathChanged.split("\n")
        every { config2.env.path} returns "extracted/node2_artifacts_2020_10_06_1140_1601977214"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val path = Path()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = path.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateDifferences)
    }

    @Test
    fun getDocumentInconsistentPathWithNonStandardFolderNames() {
        val config1 = mockk<Configuration>(relaxed = true)
        every { config1.env.data } returns env.split("\n")
        every { config1.env.path} returns "extracted/node1"

        val config2 = mockk<Configuration>(relaxed = true)
        every { config2.env.data } returns envPathChanged.split("\n")
        every { config2.env.path} returns "extracted/node2"

        val node1 = ObjectCreators.createNode(nodeName = "node1", config = config1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", config = config2)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val path = Path()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = path.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateDifferences)
    }
}