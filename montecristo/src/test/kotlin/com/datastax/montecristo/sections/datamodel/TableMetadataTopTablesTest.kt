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
import com.datastax.montecristo.helpers.toHumanCount
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class TableMetadataTopTablesTest {

    @Test
    fun getDocumentSingleTable() {
        val table = createTable("test_table", 100.0, 100000,5000.0, 10000.0, 6000, 8000.0, 20000.0)
        every { table.name } returns "test_table"

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val topTables = TableMetadataTopTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = topTables.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("test_table|100 B|100.0 k|5.0|10.0|8.0|20.0")
    }

    @Test
    fun getDocument20Table2() {

        val table1 = createTable("test_table1", 100.0, 102000, 5010.0, 10010.0, 6200, 8010.0, 10000.0)
        val table2 = createTable("test_table2", 200.0, 104000, 5020.0, 10020.0, 6000, 8020.0, 20000.0)
        val table3 = createTable("test_table3", 300.0, 106000, 5030.0, 10030.0, 6000, 8030.0, 30000.0)
        val table4 = createTable("test_table4", 400.0, 108000, 5040.0, 10040.0, 6190, 8040.0, 40000.0)
        val table5 = createTable("test_table5", 500.0, 110000, 5050.0, 10050.0, 6000, 8050.0, 50000.0)
        val table6 = createTable("test_table6", 600.0, 112000, 5060.0, 10060.0, 6000, 8060.0, 60000.0)
        val table7 = createTable("test_table7", 700.0, 114000, 5070.0, 10070.0, 6180, 8070.0, 70000.0)
        val table8 = createTable("test_table8", 800.0, 116000, 5080.0, 10080.0, 6000, 8080.0, 80000.0)
        val table9 = createTable("test_table9", 900.0, 118000, 5090.0, 10090.0, 6000, 8090.0, 90000.0)
        val table10 = createTable("test_table10", 1000.0, 120000, 5100.0, 10100.0, 6100, 8100.0, 100000.0)
        val table11 = createTable("test_table11", 1100.0, 122000, 5110.0, 10110.0, 6110, 8110.0, 110000.0)
        val table12 = createTable("test_table12", 1200.0, 124000, 5120.0, 10120.0, 6120, 8120.0, 120000.0)
        val table13 = createTable("test_table13", 1300.0, 126000, 5130.0, 10130.0, 6130, 8130.0, 130000.0)
        val table14 = createTable("test_table14", 1400.0, 128000, 5140.0, 10140.0, 6140, 8140.0, 140000.0)
        val table15 = createTable("test_table15", 1500.0, 130000, 5150.0, 10150.0, 6150, 8150.0, 150000.0)
        val table16 = createTable("test_table16", 1600.0, 132000, 5160.0, 10160.0, 6160, 8160.0, 160000.0)
        val table17 = createTable("test_table17", 1700.0, 134000, 5170.0, 10170.0, 6170, 8170.0, 170000.0)
        val table18 = createTable("test_table18", 1800.0, 136000, 5180.0, 10180.0, 5800, 8180.0, 180000.0)
        val table19 = createTable("test_table19", 1900.0, 138000, 5190.0, 10190.0, 5900, 8190.0, 190000.0)
        val table20 = createTable("test_table20", 2000.0, 140000, 5200.0, 10200.0, 6000, 8200.0, 200000.0)

        val tables = listOf(table1,table2,table3,table4,table5,table6,table7,table8,table9,table10,table11,table12,table13,table14,table15,table16,table17,table18,table19,table20)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val topTables = TableMetadataTopTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = topTables.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        // table by size, top 3
        assertThat(template).contains("test_table20|2.0 kB|140.0 k|6.0 k|5.2|10.2|8.2|200.0\n" +
                "test_table19|1.9 kB|138.0 k|5.9 k|5.19|10.19|8.19|190.0\n" +
                "test_table18|1.8 kB|136.0 k|5.8 k|5.18|10.18|8.18|180.0")
        // table by reads, top 3
        assertThat(template).contains("test_table20|2.0 kB|140.0 k|5.2|10.2|8.2|200.0\n" +
                "test_table19|1.9 kB|138.0 k|5.19|10.19|8.19|190.0\n" +
                "test_table18|1.8 kB|136.0 k|5.18|10.18|8.18|180.0")
        // table by writes, top 3 - they all go to 6.2k writes due to rounding in the human count.
        assertThat(template).contains("test_table1|100 B|6.2 k|5.01|10.01|8.01|10.0\n" +
                "test_table4|400 B|6.2 k|5.04|10.04|8.04|40.0\n" +
                "test_table7|700 B|6.2 k|5.07|10.07|8.07|70.0")
    }

    @Test
    fun getDocumentPaxosInTop10() {

        val table1 = createTable("test.table1", 100.0, 102000, 5010.0, 10010.0, 6200, 8010.0, 10000.0)
        val table2 = createTable("system.paxos", 200.0, 104000, 5020.0, 10020.0, 6000, 8020.0, 20000.0)
        val table3 = createTable("test.table3", 300.0, 106000, 5030.0, 10030.0, 6000, 8030.0, 30000.0)

        val tables = listOf(table1,table2,table3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val topTables = TableMetadataTopTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        topTables.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("The system.paxos table is within the top 10 tables for reads, this indicates that there is a significant use of light weight transactions (LWTs). We recommend evaluating the use of LWTs and where possible, alter the design of the system to reduce or eliminate the use of them.")

    }

    @Test
    fun getDocumentBatchesInTop10() {

        val table1 = createTable("test.table1", 100.0, 102000, 5010.0, 10010.0, 6200, 8010.0, 10000.0)
        val table2 = createTable("system.batches", 200.0, 104000, 5020.0, 10020.0, 6000, 8020.0, 20000.0)
        val table3 = createTable("test.table3", 300.0, 106000, 5030.0, 10030.0, 6000, 8030.0, 30000.0)

        val tables = listOf(table1,table2,table3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val topTables = TableMetadataTopTables()
        val recs: MutableList<Recommendation> = mutableListOf()

        topTables.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).contains("The system.batches table is within the top 10 tables for writes")

    }

    private fun createTable(name: String, spaceUsed: Double, reads: Long, readLatencyAvg: Double, readLatencyP99: Double, writes: Long, writeLatencyAvg: Double, writeLatencyMax: Double): Table {
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns name

        every { table.liveDiskSpaceUsed.count.sum() } returns spaceUsed
        every { table.reads.num } returns reads
        every { table.reads.toString() } returns reads.toHumanCount().toString()
        every { table.writes.num } returns writes
        every { table.writes.toString() } returns writes.toHumanCount().toString()
        every { table.readLatency.mean.max() } returns readLatencyAvg
        every { table.readLatency.p99.max() } returns readLatencyP99
        every { table.writes.num } returns writes
        every { table.writeLatency.mean.max() } returns writeLatencyAvg
        every { table.writeLatency.p99.max() } returns writeLatencyMax
        return table
    }
}