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
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class GcGraceSecondsTest {

    private val nonDefaultTemplate = "## GC grace seconds\n" +
            "\n" +
            "GC Grace Seconds is used by Cassandra to control when tombstones (deletion markers) can be purged from disk and when they should no longer be replicated in repair. It is controlled by the table property `gc_grace_seconds`.\n" +
            "\n" +
            "The default setting is 864000 (10 days). When using the default, operators are required to run a repair at least every 10 days.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">GC Grace Seconds</div></span>\n" +
            "---|---\n" +
            "test_keyspace.test_table1|8640000\n" +
            "\n" +
            "\n" +
            "\n"

    private val twoTablesNonDefaultTemplate = "## GC grace seconds\n" +
            "\n" +
            "GC Grace Seconds is used by Cassandra to control when tombstones (deletion markers) can be purged from disk and when they should no longer be replicated in repair. It is controlled by the table property `gc_grace_seconds`.\n" +
            "\n" +
            "The default setting is 864000 (10 days). When using the default, operators are required to run a repair at least every 10 days.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">GC Grace Seconds</div></span>\n" +
            "---|---\n" +
            "test_keyspace.test_table2|8640000\n" +
            "test_keyspace.test_table1|8640001\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentSingleTableDefault() {

        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_keyspace.test_table1"
        every { table.gcGrace } returns 864000

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val gcGrace = GcGraceSeconds()
        val recs: MutableList<Recommendation> = mutableListOf()

        gcGrace.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentSingleTableNonDefault() {

        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_keyspace.test_table1"
        every { table.gcGrace } returns 8640000

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val gcGrace = GcGraceSeconds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = gcGrace.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating the cause of using non-default gc_grace_seconds.  Using lower than the default can result in data loss under certain conditions.  Using greater than the default can keep tombstones around longer than desired, wasting disk space. Read [this TLP blog post](https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html) for more information.")
        assertThat(template).isEqualTo(nonDefaultTemplate)
    }

    @Test
    fun getDocumentMultipleTablesOneNonDefault() {

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_keyspace.good_table"
        every { table1.gcGrace } returns 864000
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_keyspace.test_table1"
        every { table2.gcGrace } returns 8640000

        val tables = listOf(table1,table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val gcGrace = GcGraceSeconds()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = gcGrace.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating the cause of using non-default gc_grace_seconds.  Using lower than the default can result in data loss under certain conditions.  Using greater than the default can keep tombstones around longer than desired, wasting disk space. Read [this TLP blog post](https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html) for more information.")
        assertThat(template).isEqualTo(nonDefaultTemplate)
    }

    @Test
    fun getDocumentMultipleTablesTwoNonDefault() {

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_keyspace.test_table1"
        every { table1.gcGrace } returns 8640001
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_keyspace.test_table2"
        every { table2.gcGrace } returns 8640000

        val tables = listOf(table1,table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val gcGrace = GcGraceSeconds()
        val recs: MutableList<Recommendation> = mutableListOf()
        // sort order on the output should be based on smallest grace first, so
        // table 2 outputs in the table first
        val template = gcGrace.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating the cause of using non-default gc_grace_seconds.  Using lower than the default can result in data loss under certain conditions.  Using greater than the default can keep tombstones around longer than desired, wasting disk space. Read [this TLP blog post](https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html) for more information.")
        assertThat(template).isEqualTo(twoTablesNonDefaultTemplate)
    }
}