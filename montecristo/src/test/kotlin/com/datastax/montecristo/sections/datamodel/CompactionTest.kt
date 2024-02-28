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
import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable.Companion.noCaching
import com.datastax.montecristo.fileLoaders.parsers.schema.SchemaParser
import com.datastax.montecristo.metrics.SqlLiteMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.metrics.ServerMetricList
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

class CompactionTest {

    fun metricList(node: String = "node1", number: Double = 0.0): ServerMetricList {
        val metric = Pair(node, number)
        return ServerMetricList(mutableListOf(metric))
    }

    fun table(reads: Int, writes: Int, SStablesPerReadP95: Int, name : String = "test.test"): Table {
        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val table = emptyTable(metricsDb, version, name)

        table.readLatency.count = metricList(number = reads.toDouble())
        table.writeLatency.count = metricList(number = writes.toDouble())
        table.sstablesPerReadHistogram.p95 = metricList(number = SStablesPerReadP95.toDouble())
        return table
    }

    fun emptyTable(metricsDb: SqlLiteMetricServer, version: DatabaseVersion, name: String): Table {
        return Table(metricsDb, version, listOf(), name,
                mapOf(),
                listOf(),
                CompactionDetail.stcs(),
                864000,
                "0.0",
                "0.1",
                emptyCompression(), noCaching(), .01, false, autoLoadFromMetricsDB = false, indexName = "")
    }

    private fun emptyCompression(): Compression {
        return Compression("none", HashMap())
    }

    @Test
    fun testCompactionRecommendationToLCS() {
        val table = table(10000, 1, 10)
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(table)
        assertThat(strategy.shortName).isEqualTo("LCS")
    }

    @Test
    fun testCompactionRecommendationToLCSNotEnoughSSTablesPerRead() {
        val table = table(10000, 1, 4)
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(table)
        assertThat(strategy.shortName).isEqualTo("STCS")
    }

    @Test
    fun testCompactionRecommendationToSTCS() {
        val table = table(60000, 40000, 5)
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(table)
        assertThat(strategy.shortName).isEqualTo("STCS")
    }

    @Test
    fun testCompactionRecommendationToTWCS() {
        var table = table(100, 40000, 10)
        table.compactionStrategy = CompactionDetail.dtcs()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(table)
        assertThat(strategy.shortName).isEqualTo("TWCS")
    }


    @Test
    fun testCompactionRecommendationLCSStaysAsLCS() {
        var table = table(100, 100, 1)
        table.compactionStrategy = CompactionDetail.lcs()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(table)
        assertThat(strategy.shortName).isEqualTo("LCS")
    }

    @Test
    fun testTimeSeriesIdentifiedCorrectly() {
        val tableCql = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid timeuuid,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                                AND crc_check_chance = 1.0
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99PERCENTILE';"""

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val parsedTable = SchemaParser.fromCqlString(tableCql, "us_2a_scan", metricsDb, version).getOrNull()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(parsedTable!!)
        assertThat(strategy.shortName).isEqualTo("TWCS")
    }

    @Test
    fun testTimeSeriesIdentifiedCorrectlyStaysTWCS() {
        val tableCql = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid timeuuid,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                                AND crc_check_chance = 1.0
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99PERCENTILE';"""

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val parsedTable = SchemaParser.fromCqlString(tableCql, "us_2a_scan", metricsDb, version).getOrNull()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(parsedTable!!)
        assertThat(strategy.shortName).isEqualTo("TWCS")
    }

    @Test
    fun testTimeSeriesIdentifiedCorrectlyStaysTWCSNoClusteringOrder() {
        val tableCql = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid timeuuid,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, batch_uuid))
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                                AND crc_check_chance = 1.0
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99PERCENTILE';"""

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val parsedTable = SchemaParser.fromCqlString(tableCql, "us_2a_scan", metricsDb, version).getOrNull()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(parsedTable!!)
        assertThat(strategy.shortName).isEqualTo("TWCS")
    }

    @Test
    fun testTimeSeriesIdentifiedCorrectlyWithTimestamp() {
        val tableCql = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_ts timestamp,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_ts)
                            ) WITH CLUSTERING ORDER BY (batch_ts ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                                AND crc_check_chance = 1.0
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99PERCENTILE';"""

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val parsedTable = SchemaParser.fromCqlString(tableCql, "us_2a_scan", metricsDb, version).getOrNull()
        val compaction = Compaction()
        val strategy =  compaction.getOptimalCompactionStrategy(parsedTable!!)
        assertThat(strategy.shortName).isEqualTo("TWCS")
    }

    @Test
    fun testNonTimeSeriesIdentifiedCorrectly() {
        val tableCql = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                first_clustering int,
                                batch_uuid timeuuid,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), first_clustering, batch_uuid)
                            ) WITH CLUSTERING ORDER BY (first_clustering DESC, batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                                AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                                AND crc_check_chance = 1.0
                                AND dclocal_read_repair_chance = 0.1
                                AND default_time_to_live = 0
                                AND gc_grace_seconds = 864000
                                AND max_index_interval = 2048
                                AND memtable_flush_period_in_ms = 0
                                AND min_index_interval = 128
                                AND read_repair_chance = 0.0
                                AND speculative_retry = '99PERCENTILE';"""

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.7")
        val parsedTable = SchemaParser.fromCqlString(tableCql, "us_2a_scan", metricsDb, version).getOrNull()
        val compaction = Compaction()
        val strategy = compaction.getOptimalCompactionStrategy(parsedTable!!)
        assertThat(strategy.shortName).isEqualTo("STCS")
    }

    @Test
    fun getDocumentRecommendAlreadySTCS() {

        val table = table(60000, 40000, 5)
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("There is 1 table with the p95 SSTable Per Read histogram above 5. This indicates an issue in the data model or compaction strategy choice for the table. We recommend reviewing the table, usage and compaction strategy.")
        assertThat(template).contains("""<span style="color:orange;">test.test</span>|<span style="color:orange;">STCS</span>|<span style="color:orange;">0</span>|<span style="color:orange;">0</span>|<span style="color:orange;">0</span>|<span style="color:orange;">1:1</span>|<span style="color:orange;"></span>|<span style="color:orange;">5.00</span>""")
    }

    @Test
    fun getDocumentRecommendChangeToLCS() {

        val table = table(60000, 40000, 10)
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating changing the compaction strategy to LCS for 1 table.")
        assertThat(recs[1].longForm).isEqualTo("There is 1 table with the p95 SSTable Per Read histogram above 5. This indicates an issue in the data model or compaction strategy choice for the table. We recommend reviewing the table, usage and compaction strategy.")
        assertThat(template).contains("""<span style="color:red;">test.test</span>|<span style="color:red;">STCS</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">1:1</span>|<span style="color:red;"></span>|<span style="color:red;">10.00</span>""")
        assertThat(template).contains("test.test|R:W ratio of 1:1 and/or SSTables per read p95 is 10.0")
    }

    @Test
    fun getDocumentRecommendChangeToTWCS() {

        var table = table(100, 40000, 10)
        table.compactionStrategy = CompactionDetail.dtcs()
        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(template).contains("Using DTCS")
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating changing the compaction strategy to TWCS for 1 table.")
        assertThat(recs[1].longForm).isEqualTo("There is 1 table with the p95 SSTable Per Read histogram above 5. This indicates an issue in the data model or compaction strategy choice for the table. We recommend reviewing the table, usage and compaction strategy.")
        assertThat(template).contains("""<span style="color:red;">test.test</span>|<span style="color:red;">DTCS</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0:1</span>|<span style="color:red;"></span>|<span style="color:red;">10.00</span>""")

    }

    @Test
    fun getDocumentRecommendChangeToTWCSFromDTCSTables() {

        var table1 = table(100, 40000, 10, "ks.table1")
        table1.compactionStrategy = CompactionDetail.dtcs()

        var table2 = table(250, 40000, 10, "ks.table2")
        table2.compactionStrategy = CompactionDetail.dtcs()

        val tables = listOf(table1, table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating changing the compaction strategy to TWCS for 2 tables.")
        assertThat(recs[1].longForm).isEqualTo("There are 2 tables with the p95 SSTable Per Read histogram above 5. This indicates an issue in the data model or compaction strategy choice for the tables. We recommend reviewing the tables, usage and compaction strategy.")
        assertThat(template).contains("Using DTCS")
        // the order should of reversed because its descending based on operations count
        assertThat(template).contains("""<span style="color:red;">ks.table2</span>|<span style="color:red;">DTCS</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0:1</span>|<span style="color:red;"></span>|<span style="color:red;">10.00</span>""")
        assertThat(template).contains("""<span style="color:red;">ks.table1</span>|<span style="color:red;">DTCS</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0</span>|<span style="color:red;">0:1</span>|<span style="color:red;"></span>|<span style="color:red;">10.00</span>""")
    }

    @Test
    fun getDocumentRecommendChangeToTWCSFromTimeSeries() {

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "ks.table1"
        every { table1.readLatency.count.sum() } returns 10.0
        every { table1.writeLatency.count.sum()} returns 10.0
        every { table1.sstablesPerReadHistogram.p95 } returns metricList(number = 1.0)
        every { table1.firstClusteringKeyIsTimestamp()} returns true
        every { table1.compactionStrategy.shortName } returns "STCS"

        val tables = listOf(table1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend evaluating changing the compaction strategy to TWCS for 1 table.")
        assertThat(template).contains("Identified as potential time series")
    }


    @Test
    fun getDocumentCheckLCSDefaults() {

        val metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        val version = DatabaseVersion.fromString("3.11.13")
        val table = Table(metricsDb, version, listOf(), "test.test",
            mapOf(),
            listOf(),
            CompactionDetail.lcs ( mapOf(Pair("sstable_size_in_mb", "200"))),
            864000,
            "0.0",
            "0.1",
            emptyCompression(), noCaching(), .01, false, autoLoadFromMetricsDB = false, indexName = "")
        table.readLatency.count = metricList("node1",5000.0)
        table.writeLatency.count = metricList("node1",5000.0)
        table.sstablesPerReadHistogram.p95 = metricList("node1",2.0)

        val tables = listOf(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.getUserTables() } returns tables

        val compaction = Compaction()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = compaction.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)

    }
}