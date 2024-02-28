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
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


internal class CustomTypesTest {

    @Test
    fun getDocumentNoTypes() {

        val types = emptyList<String>()
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTypes() } returns types

        val custom = CustomTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        custom.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentSomeTypesCassandra30() {

        val types = listOf("TypeA", "TypeB")
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTypes() } returns types
        every { cluster.databaseVersion } returns DatabaseVersion.latest30()
        val custom = CustomTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        custom.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We advise against using UDTs before Cassandra 3.11 due to upgrade bugs affecting 3.0 SSTables.")
    }

    @Test
    fun getDocumentSomeTypesAllFrozenCassandra311() {

        val types = listOf("TypeA", "TypeB")
        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.hasNonFrozenCollections() } returns false
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTypes() } returns types
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.schema.tables } returns tables
        val custom = CustomTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        custom.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentSomeTypesNotFrozenCassandra311() {

        val types = listOf("TypeA", "TypeB")
        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.hasNonFrozenTypes() } returns true
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTypes() } returns types
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        every { cluster.schema.getUserTables() } returns tables
        val custom = CustomTypes()
        val recs: MutableList<Recommendation> = mutableListOf()

        custom.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
    }

}