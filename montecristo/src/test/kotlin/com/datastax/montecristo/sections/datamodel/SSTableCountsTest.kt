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
import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable
import com.datastax.montecristo.metrics.SqlLiteMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.metrics.SSTableCount
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.CompactionDetail
import com.datastax.montecristo.model.schema.Compression
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SSTableCountsTest {

    fun createTable(tableName: String = "test.test"): Table {
        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        return Table(metricsDb, version, listOf(), tableName,
                mapOf(),
                listOf(),
                CompactionDetail.stcs(),
                864000,
                "0.0",
                "0.1",
                Compression("none", HashMap()), ParsedCreateTable.noCaching(), .01, false, autoLoadFromMetricsDB = false, indexName = "")
    }

    @Test
    fun testCheckHighTableCountNoticed() {

        val table1 = createTable("test.test1")
        val table2 = createTable("test.test2")
        val stcsTables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns stcsTables

        val highSStableTable1 = SSTableCount("node1", "test", "test2", 50)
        val highSSTableCounts = listOf(highSStableTable1)
        every { cluster.metricServer.getSStableCounts(20) } returns highSSTableCounts


        val ssTableDoc = SSTableCounts()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ssTableDoc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you review the compaction configuration and settings for 1 table(s) to reduce the SSTable count.")
        assertThat(template).contains("node1|test|test2|50")
    }

    @Test
    fun testCheckNoHighTableCount() {

        val table1 = createTable("test.test1")
        val table2 = createTable("test.test2")
        val stcsTables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns stcsTables

        val highSSTableCounts = emptyList<SSTableCount>()
        every { cluster.metricServer.getSStableCounts(20) } returns highSSTableCounts

        val ssTableDoc = SSTableCounts()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ssTableDoc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun testCheckHighTableCountMultiNode() {

        val table1 = createTable("test.test1")
        val stcsTables = listOf(table1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns stcsTables

        val highSStableTable1 = SSTableCount("node1", "test", "test1", 50)
        val highSStableTable2 = SSTableCount("node2", "test", "test1", 51)
        val highSSTableCounts = listOf(highSStableTable1, highSStableTable2)
        every { cluster.metricServer.getSStableCounts(20) } returns highSSTableCounts


        val ssTableDoc = SSTableCounts()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = ssTableDoc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend that you review the compaction configuration and settings for 1 table(s) to reduce the SSTable count.")
        assertThat(template).contains("node1|test|test1|50")
        assertThat(template).contains("node2|test|test1|51")
    }

}