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
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SecondaryIndexesTest {

    @Test
    fun getDocumentClean() {
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserIndexes() } returns emptyList()

        val secondary = SecondaryIndexes()
        val recs: MutableList<Recommendation> = mutableListOf()

        secondary.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentHasSecondary() {

        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table1"
        every { table.getKeyspace() } returns "some_keyspace"
        every { table.is2i} returns true
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserIndexes() } returns tables

        val secondary = SecondaryIndexes()
        val recs: MutableList<Recommendation> = mutableListOf()

        secondary.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(recs[0].longForm).isEqualTo("We recommend reviewing the model of tables using secondary indexes. There is one secondary index in the data model.")
    }
}