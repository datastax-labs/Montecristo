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

internal class UnusedTablesTest {

    private val templateUnusedTables = "## Unused Tables\n" +
            "\n" +
            "A large number of tables in Cassandra can directly impact the performance of the cluster. Typically, you should have no more than 200 actively used tables in a cluster. Should the number of active unused tables increase above 500, the cluster may be prone to performance degradation and failure.\n" +
            "\n" +
            "The problem arises because every table uses approximately 1 MB of memory for metadata. For each table acted on, a memtable representation is allocated. Tables with large amounts of data also increase pressure on memory by storing more data for the bloom filter and other auxiliary data structures.  \n" +
            "Also, each keyspace causes additional overhead in JVM memory. All of this together, impacts the performance of Cassandra. \n" +
            "\n" +
            "For that reason, it is recommended to drop tables that are unused.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Space Used</div></span>\n" +
            "---|---\n" +
            "test_table|5.0 GB\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "---\n" +
            "\n" +
            "**Noted for reference**: _We recommend dropping unused keyspaces and tables to facilitate operations._\n" +
            "\n" +
            "---  \n" +
            "\n"

    private val templateTwoUnusedTables = "## Unused Tables\n" +
            "\n" +
            "A large number of tables in Cassandra can directly impact the performance of the cluster. Typically, you should have no more than 200 actively used tables in a cluster. Should the number of active unused tables increase above 500, the cluster may be prone to performance degradation and failure.\n" +
            "\n" +
            "The problem arises because every table uses approximately 1 MB of memory for metadata. For each table acted on, a memtable representation is allocated. Tables with large amounts of data also increase pressure on memory by storing more data for the bloom filter and other auxiliary data structures.  \n" +
            "Also, each keyspace causes additional overhead in JVM memory. All of this together, impacts the performance of Cassandra. \n" +
            "\n" +
            "For that reason, it is recommended to drop tables that are unused.\n" +
            "\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Space Used</div></span>\n" +
            "---|---\n" +
            "test_table2|8.0 GB\n" +
            "test_table1|5.0 GB\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "---\n" +
            "\n" +
            "**Noted for reference**: _We recommend dropping unused keyspaces and tables to facilitate operations._\n" +
            "\n" +
            "---  \n" +
            "\n"
    @Test
    fun getDocumentClean() {
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns emptyList<Table>()

        val unused = UnusedTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = unused.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("There are no unused tables.")
    }

    @Test
    fun getDocumentUnusedTables() {
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.totalDiskSpaceUsed.count.sum() } returns 5_000_000_000.0
        every { table.readLatency.count.sum().toLong() } returns 0
        every { table.writeLatency.count.sum().toLong() } returns 0
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        every { cluster.schema.getUnusedUserTables() } returns tables
        val unused = UnusedTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = unused.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).isEqualTo(templateUnusedTables)
    }

    @Test
    fun getDocumentTwoUnusedTables() {
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.totalDiskSpaceUsed.count.sum() } returns 5_000_000_000.0
        every { table1.readLatency.count.sum().toLong() } returns 0
        every { table1.writeLatency.count.sum().toLong() } returns 0
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.totalDiskSpaceUsed.count.sum() } returns 8_000_000_000.0
        every { table2.readLatency.count.sum().toLong() } returns 0
        every { table2.writeLatency.count.sum().toLong() } returns 0
        // output should reverse the tables, table 2 uses more space
        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        every { cluster.schema.getUnusedUserTables() } returns tables
        val unused = UnusedTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = unused.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).isEqualTo(templateTwoUnusedTables)
    }
}