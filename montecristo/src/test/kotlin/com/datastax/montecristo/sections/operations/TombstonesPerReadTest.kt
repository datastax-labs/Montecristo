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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class TombstonesPerReadTest {

    private val tombstonesTemplate = "## Tombstones Per Read\n" +
        "\n" +
        "Tombstones per read are a metric tracked per table.   \n" +
        "\n" +
        "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Tombstones (p99)</div></span>|<span><div style=\"text-align:left;\">Tombstones (max)</div></span>|<span><div style=\"text-align:left;\">Read Latency (ms, p99)</div></span>\n" +
        "---|---|---|---\n" +
        "test_table|20.00|100.00|5.50\n" +
        "\n" +
        "\n"

    private val tombstonesTwoTablesTemplate = "## Tombstones Per Read\n" +
            "\n" +
            "Tombstones per read are a metric tracked per table.   \n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Tombstones (p99)</div></span>|<span><div style=\"text-align:left;\">Tombstones (max)</div></span>|<span><div style=\"text-align:left;\">Read Latency (ms, p99)</div></span>\n" +
            "---|---|---|---\n" +
            "test_table2|50.00|100.00|5.50\n" +
            "test_table1|20.00|100.00|5.50\n" +
            "\n" +
            "\n"
    @Test
    fun getDocumentClean() {
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.tombstoneScannedHistogram.p99.max() } returns 0.0
        every { table.tombstoneScannedHistogram.max.max() } returns 0.0
        every { table.readLatency.p99.average() } returns 0.01
        val tables = listOf(table)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        val searcher = mockk<Searcher>(relaxed = true)
        val tombstones = TombstonesPerRead()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tombstones.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo("")
    }

    @Test
    fun getDocumentTombstones() {
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.tombstoneScannedHistogram.p99.max() } returns 20.0
        every { table.tombstoneScannedHistogram.max.max() } returns 100.0
        every { table.readLatency.p99.average() } returns 5500.0
        val tables = listOf(table)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        val searcher = mockk<Searcher>(relaxed = true)
        val tombstones = TombstonesPerRead()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(tombstonesTemplate)
    }

    @Test
    fun getDocumentTwoTablesTombstones() {
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.tombstoneScannedHistogram.p99.max() } returns 20.0
        every { table1.tombstoneScannedHistogram.max.max() } returns 100.0
        every { table1.readLatency.p99.average() } returns 5500.0
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.tombstoneScannedHistogram.p99.max() } returns 50.0
        every { table2.tombstoneScannedHistogram.max.max() } returns 100.0
        every { table2.readLatency.p99.average() } returns 5500.0
        // the output should reverse the order since tables 2 has a higher pp value
        val tables = listOf(table1,table2)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables
        val searcher = mockk<Searcher>(relaxed = true)
        val tombstones = TombstonesPerRead()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(tombstonesTwoTablesTemplate)
    }

    @Test
    fun getDocumentTombstonesDecimalPlaces() {
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.tombstoneScannedHistogram.p99.max() } returns 20.123456789
        every { table.tombstoneScannedHistogram.max.max() } returns 100.987654321
        every { table.readLatency.p99.average() } returns 5500.0
        val tables = listOf(table)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val tombstones = TombstonesPerRead()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val template = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("test_table|20.12|100.99|5.50\n")
    }
}