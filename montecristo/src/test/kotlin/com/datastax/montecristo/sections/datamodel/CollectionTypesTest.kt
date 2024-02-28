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
import org.assertj.core.api.Assertions
import org.junit.Test

internal class CollectionTypesTest() {

    // This overall output is huge, so we will do a contains to look for the
    // single part of the template that changes (which is near the end)
    private val noCollectionsTemplate = "There are no tables using collections."

    private val containsListsTemplate = "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Maps</div></span>|<span><div style=\"text-align:left;\">Sets</div></span>|<span><div style=\"text-align:left;\">Lists</div></span>\n" +
            "---|---|---|---\n" +
            "test_table|11|12|13"

    @Test
    fun getDocumentNoCollections() {
        val tables: MutableList<Table> = mutableListOf()
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.hasNonFrozenCollections() } returns false
        tables.add(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val collections = CollectionTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = collections.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).contains(noCollectionsTemplate)
    }

    @Test
    fun getDocumentSomeCollections() {
        val tables: MutableList<Table> = mutableListOf()
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.hasNonFrozenCollections() } returns true
        every { table.getMaps().count() } returns 11
        every { table.getSets().count() } returns 12
        every { table.getLists().count() } returns 13
        tables.add(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val collections = CollectionTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = collections.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(2)
        Assertions.assertThat(recs.first().priority).isEqualTo(RecommendationPriority.NEAR)
        Assertions.assertThat(recs.last().priority).isEqualTo(RecommendationPriority.NEAR)
        Assertions.assertThat(recs.first().longForm).isEqualTo("We recommend reviewing the use of lists in the data model. We found 13 lists in a single table in the schema. For each list, assess whether it can be frozen, replaced with a set or map for a list under 100 elements, or with a dedicated table for lists over 100 elements.")
        Assertions.assertThat(recs.last().longForm).isEqualTo("We recommend using a frozen variant of a collection where it is used for immutable data (inserted only once and never updated, or updated as a whole). This is so Cassandra will automatically serialize content into a single binary value. It will remove the overhead of using a collection type in exchange for the ability to perform a partial update of the value.")

        Assertions.assertThat(template).contains(containsListsTemplate)
    }
}