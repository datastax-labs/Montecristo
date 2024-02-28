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
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

/*
Test we give the right advice for the various versions of Cassandra.
 */
class DatabaseVersionTest {
    @Test
    fun testUnsupportedVersions() {
        var advice = Version().getAdvice(com.datastax.montecristo.model.versions.DatabaseVersion.fromString("2.0.0."), false)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(com.datastax.montecristo.model.versions.DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(com.datastax.montecristo.model.versions.DatabaseVersion.fromString("2.0.0.").toString())
        assertThat(advice[0].longForm).contains("unsupported")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latest20().toString())


        advice = Version().getAdvice(DatabaseVersion.latest12(), false)!!
        assertThat(advice.size).isEqualTo(1)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest12().toString())
        assertThat(advice[0].longForm).contains("unsupported")

        advice = Version().getAdvice(DatabaseVersion.fromString("3.6"), false)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(DatabaseVersion.fromString("3.6").toString())
        assertThat(advice[0].longForm).contains("unsupported")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latest311().toString())

        advice = Version().getAdvice(DatabaseVersion.fromString("5.0.0", true), true)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(" no longer supported")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latestDSE50().toString())
    }

    @Test
    fun testSupportedVersions() {
        var advice = Version().getAdvice(DatabaseVersion.fromString("3.0.0"), false)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(DatabaseVersion.fromString("3.0.0").toString())
        assertThat(advice[0].longForm).contains(" support")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latest30().toString())


        advice = Version().getAdvice(DatabaseVersion.fromString("3.11.0"), false)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(DatabaseVersion.fromString("3.11.0").toString())
        assertThat(advice[0].longForm).contains(" support")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latest311().toString())

        advice = Version().getAdvice(DatabaseVersion.fromString("4.0.0"), false)!!
        assertThat(advice.size).isEqualTo(2)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(advice[0].longForm).contains(DatabaseVersion.fromString("4.0.0").toString())
        assertThat(advice[0].longForm).contains(" support")
        assertThat(advice[1].longForm).contains(DatabaseVersion.latest40().toString())

        advice = Version().getAdvice(DatabaseVersion.fromString("4.1.0"), false)!!
        assertThat(advice.size).isEqualTo(1)
        assertThat(advice[0].longForm).contains(DatabaseVersion.latest41().toString())

        advice = Version().getAdvice(DatabaseVersion.latest41(), false)!!
        assertThat(advice.size).isEqualTo(0)
    }


    @Test
    fun multipleVersions() {

        val node1 = ObjectCreators.createNode(nodeName = "node1", databaseVersion = DatabaseVersion.latest311())
        val node2 = ObjectCreators.createNode(nodeName = "node2", databaseVersion = DatabaseVersion.fromString("3.6"))
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.isDse } returns false
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val version = Version()
        var response = version.getDocument(cluster, searcher, recs, ExecutionProfile.default())

        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(recs[1].longForm).contains("We recommend that a single version of Cassandra is used within the cluster.")
        assertThat(response).contains("${DatabaseVersion.latest311()}|1")
        assertThat(response).contains("3.6|1")
    }

    @Test
    fun singleVersion() {

        val node1 = ObjectCreators.createNode(nodeName = "node1", databaseVersion = DatabaseVersion.latest311())
        val node2 = ObjectCreators.createNode(nodeName = "node2", databaseVersion = DatabaseVersion.latest311())
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.isDse } returns false
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val version = Version()
        var response = version.getDocument(cluster, searcher, recs, ExecutionProfile.default())

        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains(DatabaseVersion.latest41().toString())
        assertThat(response).contains("The version of Cassandra currently in use is ${DatabaseVersion.latest311()}.")
    }

    @Test
    fun singleVersionDSEv50() {

        val node1 = ObjectCreators.createNode(nodeName = "node1", databaseVersion = DatabaseVersion.latestDSE50())
        val node2 = ObjectCreators.createNode(nodeName = "node2", databaseVersion = DatabaseVersion.latestDSE50())
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latestDSE50()
        every { cluster.isDse } returns true
        every { cluster.nodes } returns nodeList

        val recs: MutableList<Recommendation> = mutableListOf()
        val version = Version()
        version.getDocument(cluster, searcher, recs, ExecutionProfile.default())

        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("6.8")
        assertThat(recs[0].longForm).contains("https://www.datastax.com/legal/supported-software")
    }
}
