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

internal class MaterializedViewsTest {

    private val noMVsTemplate = "## Materialized Views\n" +
            "\n" +
            "Materialized Views were added in Cassandra 3.0 but have not yet reached a point of stability, and have recently been marked as experimental.  They have numerous issues related to data integrity and lack tooling to validate or repair the data in the view against the base table.\n" +
            "\n" +
            "Materialized views also have a tendency to result in extremely large partitions if not very carefully planned out.  They must be approached with the same data modeling best practices as any other table.\n" +
            "\n" +
            "Nodes down longer than the hint window can result in data becoming out of sync, and repairing materialized views takes orders of magnitude longer than repairing a normal table.\n" +
            "\n" +
            "We do not recommend using materialized views at this time.\n" +
            "\n" +
            "---\n" +
            "\n" +
            "**Noted for reference**: _No materialized views are in use at this time and we recommend that you continue to not use them._\n" +
            "\n" +
            "---"

    private val twoMVsTemplate = "## Materialized Views\n" +
            "\n" +
            "Materialized Views were added in Cassandra 3.0 but have not yet reached a point of stability, and have recently been marked as experimental.  They have numerous issues related to data integrity and lack tooling to validate or repair the data in the view against the base table.\n" +
            "\n" +
            "Materialized views also have a tendency to result in extremely large partitions if not very carefully planned out.  They must be approached with the same data modeling best practices as any other table.\n" +
            "\n" +
            "Nodes down longer than the hint window can result in data becoming out of sync, and repairing materialized views takes orders of magnitude longer than repairing a normal table.\n" +
            "\n" +
            "We do not recommend using materialized views at this time.\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">MV</div></span>|<span><div style=\"text-align:left;\">Compaction Strategy</div></span>|<span><div style=\"text-align:left;\">Max Partition Size</div></span>\n" +
            "---|---|---\n" +
            "test_table1|STCS|100.0 kB\n" +
            "test_table2|STCS|200.0 kB\n"

    @Test
    fun getDocumentNoMVs() {

        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table1"
        every { table.isMV } returns false
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val mv = MaterializedViews()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mv.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // never issues a recommendation
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(noMVsTemplate)
    }

    @Test
    fun getDocumentTwoMVs() {

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.isMV } returns true
        every { table1.compactionStrategy.shortName } returns "STCS"
        every { table1.maxPartitionSize.value.max() } returns 100000.0
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.isMV } returns true
        every { table2.compactionStrategy.shortName } returns "STCS"
        every { table2.maxPartitionSize.value.max() } returns 200000.0
        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val mv = MaterializedViews()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = mv.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // there will be a recommendation to remove the MV
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).isEqualTo(twoMVsTemplate)
        assertThat(recs[0].longForm).isEqualTo("We recommend that a data model review is undertaken to eliminate the use of 2 materialized view(s).")
    }
}