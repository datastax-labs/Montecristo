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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Keyspace
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class OpsCenterTest {

    @Test
    fun getDocumentNoOpsCenter() {

        val ks1 = Keyspace("Foo", emptyList(), "org.apache.cassandra.locator.NetworkTopologyStrategy")
        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val ops = OpsCenter()
        val recs: MutableList<Recommendation> = mutableListOf()

        ops.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentOpsCenter() {

        val ks1 = Keyspace("OpsCenter", emptyList(), "org.apache.cassandra.locator.NetworkTopologyStrategy")
        val keyspaces = listOf(ks1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.keyspaces } returns keyspaces

        val ops = OpsCenter()
        val recs: MutableList<Recommendation> = mutableListOf()

        ops.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
    }
}