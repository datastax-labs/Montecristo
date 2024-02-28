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

package com.datastax.montecristo.sections.schema

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.nodetool.GossipInfo
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.datamodel.SchemaVersions
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SchemaVersionsTest {

    @Test
    fun getDocumentAgreementOnVersions() {
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.isSchemaInAgreement } returns true

        val agreement = SchemaVersions()
        val recs: MutableList<Recommendation> = mutableListOf()

        val result = agreement.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(result).contains("The schema versions are all in agreement.")
    }

    @Test
    fun getDocumentDisagreementOnCqlHashAgreementOnGossip() {
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.isSchemaInAgreement } returns false
        val versionMap = mutableMapOf<String, GossipInfo>()
        versionMap.put(
            "node1",
            GossipInfo(
                0,
                0,
                "NORMAL",
                "abcd",
                "dc1",
                "rack1",
                "3.11.10",
                "192.168.1.1",
                "192.168.1.1",
                Utils.parseJson("{}"),
                "abcd4321-5ad1-47b2-a197-0d0be854c511",
                true
            )
        )
        versionMap.put(
            "node2",
            GossipInfo(
                0,
                0,
                "NORMAL",
                "abcd",
                "dc1",
                "rack1",
                "3.11.10",
                "192.168.1.2",
                "192.168.1.2",
                Utils.parseJson("{}"),
                "abcd1234-5ad1-47b2-a197-0d0be854c511",
                true
            )
        )
        every { cluster.nodes.first().gossipInfo } returns versionMap

        val agreement = SchemaVersions()
        val recs: MutableList<Recommendation> = mutableListOf()

        val result = agreement.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(result).contains("The schema versions are all in agreement.")
    }

    @Test
    fun getDocumentDisagreementOnVersions() {
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.isSchemaInAgreement } returns false
        val versionMap = mutableMapOf<String, GossipInfo>()
        versionMap.put(
            "node1",
            GossipInfo(
                0,
                0,
                "NORMAL",
                "abcd",
                "dc1",
                "rack1",
                "3.11.10",
                "192.168.1.1",
                "192.168.1.1",
                Utils.parseJson("{}"),
                "abcd4321-5ad1-47b2-a197-0d0be854c511",
                true
            )
        )
        versionMap.put(
            "node2",
            GossipInfo(
                0,
                0,
                "NORMAL",
                "efgh",
                "dc1",
                "rack1",
                "3.11.10",
                "192.168.1.2",
                "192.168.1.2",
                Utils.parseJson("{}"),
                "abcd1234-5ad1-47b2-a197-0d0be854c511",
                true
            )
        )
        every { cluster.nodes.first().gossipInfo } returns versionMap

        val agreement = SchemaVersions()
        val recs: MutableList<Recommendation> = mutableListOf()

        val result = agreement.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(result).contains("node1|abcd")
        assertThat(result).contains("node2|efgh")
    }
}