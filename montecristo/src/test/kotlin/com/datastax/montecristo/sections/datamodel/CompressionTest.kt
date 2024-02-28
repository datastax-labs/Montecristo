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

internal class CompressionTest {

    @Test
    fun getDocumentNoRecommendation() {

        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.compression.getShortName() } returns "L4ZCompression"
        every { table.compression.getChunkLength() } returns "16" // this is used in the logic
        every { table.compressionRatio.value.max() } returns 0.5
        every { table.getRWRatioHuman() } returns "1:10"
        every { table.meanPartitionSize.value.averageBytesAsHumanReadable()} returns "10 MB"
        every { table.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table.readLatency.count.sum() } returns 1.0
        every { table.readLatency.p99.max() } returns 1.0
        every { table.sstablesPerReadHistogram.p99.max() } returns 5.0
        every { table.compressionRatio.value.max() } returns 0.6

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("test_table1|L4ZCompression|16|1:10|10 MB|100.0 GB")
    }

    @Test
    fun getDocumentRecommendChangingCompression() {

        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.compression.getShortName() } returns "L4ZCompression"
        every { table.compression.getChunkLength() } returns "64" // this is used in the logic
        every { table.compressionRatio.value.max() } returns 0.89
        every { table.getRWRatioHuman() } returns "1:10"
        every { table.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table.readLatency.count.sum() } returns 1.0
        every { table.readLatency.p99.max() } returns 1.0
        every { table.sstablesPerReadHistogram.p99.max() } returns 5.0

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend lowering the compression chunk length down to 16kb," +
                " for all the tables that have a high read traffic, where the mean partition size is smaller than 16kb." +
                " Several tables are using the default chunk length of 64kb which in most cases involves more I/O" +
                " usage than necessary. You can do this by altering the schema and running a subsequent" +
                " `nodetool upgradesstables -a` to rewrite all SSTables to disk.")
        assertThat(template).contains("test_table1|L4ZCompression|64|1:10|10 MB|100.0 GB|11.0%")
    }

    @Test
    fun getDocumentRecommendDisablingCompression() {

        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.compressionRatio.value.max() } returns 1.01
        every { table.name } returns "test_table1"
        every { table.compression.getShortName() } returns "L4ZCompression"
        every { table.compression.getChunkLength() } returns "16" // this is used in the logic
        every { table.getRWRatioHuman() } returns "1:10"
        every { table.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table.readLatency.count.sum() } returns 1.0
        every { table.readLatency.p99.max() } returns 1.0
        every { table.sstablesPerReadHistogram.p99.max() } returns 5.0

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        val countOfTablesWithBadCompression = 1
        assertThat(recs[0].longForm).isEqualTo("$countOfTablesWithBadCompression table(s) are using compression but not gaining any advantage from doing so. We recommend reviewing and removing compression on these tables.")
        assertThat(template).contains("<span style=\"color:red;\">test_table1</span>|<span style=\"color:red;\">L4ZCompression</span>|<span style=\"color:red;\">16</span>|<span style=\"color:red;\">1:10</span>|<span style=\"color:red;\">10 MB</span>|<span style=\"color:red;\">100.0 GB</span>|<span style=\"color:red;\">-1.0%</span>\n")
    }

    @Test
    fun getDocumentRecommendDisablingCompressionEdgeCase() {

        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.compressionRatio.value.max() } returns 0.9
        every { table.name } returns "test_table1"
        every { table.compression.getShortName() } returns "L4ZCompression"
        every { table.compression.getChunkLength() } returns "16" // this is used in the logic
        every { table.getRWRatioHuman() } returns "1:10"
        every { table.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table.readLatency.count.sum() } returns 1.0
        every { table.readLatency.p99.max() } returns 1.0
        every { table.sstablesPerReadHistogram.p99.max() } returns 5.0

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        val countOfTablesWithBadCompression = 1
        assertThat(recs[0].longForm).isEqualTo("$countOfTablesWithBadCompression table(s) are using compression but not gaining any advantage from doing so. We recommend reviewing and removing compression on these tables.")
        assertThat(template).contains("<span style=\"color:orange;\">test_table1</span>|<span style=\"color:orange;\">L4ZCompression</span>|<span style=\"color:orange;\">16</span>|<span style=\"color:orange;\">1:10</span>|<span style=\"color:orange;\">10 MB</span>|<span style=\"color:orange;\">100.0 GB</span>|<span style=\"color:orange;\">10.0%</span>")
    }

    @Test
    fun getDocumentRecommendChangingCompressionTwoTables() {

        val table1 = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table1.name } returns "test_table1"
        every { table1.compression.getShortName() } returns "L4ZCompression"
        every { table1.compression.getChunkLength() } returns "64" // this is used in the logic
        every { table1.compressionRatio.value.max() } returns 0.5
        every { table1.getRWRatioHuman() } returns "1:10"
        every { table1.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table1.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table1.readLatency.count.sum() } returns 1.0
        every { table1.readLatency.p99.max() } returns 1.0
        every { table1.sstablesPerReadHistogram.p99.max() } returns 5.0


        val table2 = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table2.name } returns "test_table2"
        every { table2.compression.getShortName() } returns "L4ZCompression"
        every { table2.compression.getChunkLength() } returns "64" // this is used in the logic
        every { table2.compressionRatio.value.max() } returns 0.5
        every { table2.getRWRatioHuman() } returns "1:10"
        every { table2.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table2.liveDiskSpaceUsed.count.sum()} returns 200000000000.0
        every { table2.readLatency.count.sum() } returns 5.0
        every { table2.readLatency.p99.max() } returns 1.0
        every { table2.sstablesPerReadHistogram.p99.max() } returns 5.0
        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend lowering the compression chunk length down to 16kb," +
                " for all the tables that have a high read traffic, where the mean partition size is smaller than 16kb." +
                " Several tables are using the default chunk length of 64kb which in most cases involves more I/O" +
                " usage than necessary. You can do this by altering the schema and running a subsequent" +
                " `nodetool upgradesstables -a` to rewrite all SSTables to disk.")
        // Right now the data is sorted inside the class to get the top 10 to check whether to make
        // the recommendation, but it doesn't actually output them.
        // I suspect this is an error in the rule - and it should be outputting a table, this
        // test will then come in more use when checking the sort order.
        assertThat(template).contains("test_table1|L4ZCompression|64|1:10|10 MB|100.0 GB")
        assertThat(template).contains("test_table2|L4ZCompression|64|1:10|10 MB|200.0 GB")
    }

    @Test
    fun getDocumentNonDefaultChunkSizeRecommendChangingCompression() {

        val table = mockk<Table>(relaxed = true)
        // most of these settings impact the output but not the calculation logic
        every { table.name } returns "test_table1"
        every { table.compression.getShortName() } returns "L4ZCompression"
        every { table.compression.getChunkLength() } returns "32" // this is used in the logic
        every { table.getRWRatioHuman() } returns "1:10"
        every { table.meanPartitionSize.value.averageBytesAsHumanReadable()}  returns "10 MB"
        every { table.liveDiskSpaceUsed.count.sum()} returns 100000000000.0
        every { table.readLatency.count.sum() } returns 1.0
        every { table.readLatency.p99.max() } returns 1.0
        every { table.sstablesPerReadHistogram.p99.max() } returns 5.0

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()

        val compression = Compression()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compression.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend lowering the compression chunk length down to 16kb," +
                " for all the tables that have a high read traffic, where the mean partition size is smaller than 16kb." +
                " You can do this by altering the schema and running a subsequent" +
                " `nodetool upgradesstables -a` to rewrite all SSTables to disk.")
        assertThat(template).contains("test_table1|L4ZCompression|32|1:10|10 MB|100.0 GB")
    }
}