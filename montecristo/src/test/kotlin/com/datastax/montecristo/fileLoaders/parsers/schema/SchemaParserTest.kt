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

package com.datastax.montecristo.fileLoaders.parsers.schema

import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.metrics.SqlLiteMetricServer
import com.datastax.montecristo.model.versions.DatabaseVersion
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertTrue
import org.junit.Before

import org.junit.Test
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

internal class SchemaParserTest() {

    lateinit var metricsDb: SqlLiteMetricServer

    @Before
    fun setUp() {
        metricsDb = mockk<SqlLiteMetricServer>(relaxed = true)
        //every { metricsDb.datacenter } returns dc

    }

    @Test
    fun testSimpleParse() {
        val schemaTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/schema/schema.cql").reader().readText()
        val version = DatabaseVersion.fromString ("3.11.7")
        val schema = SchemaParser.parse(schemaTxt, metricsDb, version, true)
        assertEquals(1, schema.keyspaces.size)
        assertNotNull(schema.getKeyspace("testks"))
        assertTrue(schema.getKeyspace("testks")?.name == "testks")
        assertTrue(schema.tables?.size > 0)
        assertNotNull(schema.getTable("testks.ip_subnet"))
    }

    @Test
    fun testParseWith2IParse() {
        val schemaTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/schema/schema-with2i.cql").reader().readText()
        val version = DatabaseVersion.fromString ("3.9.0")
        val schema = SchemaParser.parse(schemaTxt, metricsDb, version, true)
        assertEquals(1, schema.keyspaces.size)
        assertNotNull(schema.getKeyspace("testks"))
        assertTrue(schema.getKeyspace("testks")?.name == "testks")
        val tables = schema.tables.filter { !it.is2i && !it.isMV }
        assertTrue(tables.size == 1)
        assertNotNull(schema.getTable("testks.log"))
    }


    @Test
    fun testKeySpaceParse() {
        val schemaTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/schema/schema.cql").reader().readLines()
        val keyspaces = SchemaParser.getKeyspaces(schemaTxt)
        assertEquals(1, keyspaces.size)
        assertTrue(keyspaces.first().name == "testks")
    }

    @Test
    fun testReadingTablesMetrics() {
        val schemaTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/schema/schema.cql").reader().readText()
        val databaseVersion: DatabaseVersion = DatabaseVersion.fromString("3.11.7")
        val schema = SchemaParser.parse(schemaTxt, metricsDb, databaseVersion, true)
        assertTrue(schema.keyspaces.size == 1)
        assertNotNull(schema.getKeyspace("testks"))
        // given we have attached no real metrics, everything would be unused
        assertTrue(schema.tables.filter {
            it.readLatency.count.sum().toLong() == 0.toLong() && it.writeLatency.count.sum().toLong() == 0.toLong()
        }.size > 0)
    }


    @Test
    fun testParsingDSESchemaGetUserObjectsOnly() {
        val schemaTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/schema/dse-keyspace-schema.cql").reader().readText()
        val databaseVersion: DatabaseVersion = DatabaseVersion.fromString("3.11.7")
        val schema = SchemaParser.parse(schemaTxt, metricsDb, databaseVersion, true)
        assertThat(schema.keyspaces.size).isEqualTo(16)
        assertNotNull(schema.getKeyspace("another_enterprise_keyspacename"))
        // now check how many keyspaces when we filter the system ones out
        assertThat(schema.getUserKeyspaces().size).isEqualTo(2)
        assertThat(schema.tables.size).isEqualTo(2)
        assertThat(schema.getUserTables().size).isEqualTo(1)
    }

    @Test
    fun testParsingDSECustomType() {
        val schemaTxt = "CREATE TABLE sensor_data_ks.states (\n" +
                "    client_id bigint,\n" +
                "    device_id bigint,\n" +
                "    appeui text,\n" +
                "    decoding_status boolean,\n" +
                "    decryption_status boolean,\n" +
                "    deleted_at timestamp,\n" +
                "    deveui text,\n" +
                "    disabled boolean,\n" +
                "    downlink_count bigint,\n" +
                "    geo_hash set<text>,\n" +
                "    geo_point 'org.apache.cassandra.db.marshal.PointType',\n" +
                "    geo_type text,\n" +
                "    geo_zones set<text>,\n" +
                "    groups list<bigint>,\n" +
                "    last_downlink timestamp,\n" +
                "    last_downlink_try timestamp,\n" +
                "    last_external timestamp,\n" +
                "    last_geoloc timestamp,\n" +
                "    last_join timestamp,\n" +
                "    last_status timestamp,\n" +
                "    last_update map<text, timestamp>,\n" +
                "    last_uplink timestamp,\n" +
                "    latitude double,\n" +
                "    longitude double,\n" +
                "    name text,\n" +
                "    precision double,\n" +
                "    profile bigint,\n" +
                "    proto_data map<text, text>,\n" +
                "    protocol_data text,\n" +
                "    solr_query text,\n" +
                "    status text,\n" +
                "    uplink_count bigint,\n" +
                "    values map<text, text>,\n" +
                "    values_version smallint,\n" +
                "    vd_ map<text, double>,\n" +
                "    vs_ map<text, text>,\n" +
                "    zone bigint,\n" +
                "    PRIMARY KEY ((client_id, device_id))\n" +
                ") WITH read_repair_chance = 0.0\n" +
                "    AND dclocal_read_repair_chance = 0.1\n" +
                "    AND gc_grace_seconds = 864000\n" +
                "    AND bloom_filter_fp_chance = 0.01\n" +
                "    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\n" +
                "    AND comment = ''\n" +
                "    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }\n" +
                "    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }\n" +
                "    AND default_time_to_live = 0\n" +
                "    AND speculative_retry = '99PERCENTILE'\n" +
                "    AND min_index_interval = 128\n" +
                "    AND max_index_interval = 2048\n" +
                "    AND crc_check_chance = 1.0\n" +
                "    AND cdc = false\n" +
                "    AND memtable_flush_period_in_ms = 0\n" +
                "    AND nodesync = { 'enabled' : 'true' };\n"
        val version = DatabaseVersion.fromString("6.7.8", true)
        val schema = SchemaParser.parse(schemaTxt, metricsDb, version, true)
        assertThat(schema.keyspaces.size).isEqualTo(0)
        assertThat(schema.tables.size).isEqualTo(1)
    }

    @Test
    fun createSchemaTestSchemaFileInAgreement() {
        // test schema files in agreement
        val schemaFile1 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.1/schema1").path)
        val schemaFile2 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.2/schema1").path)

        val schemaFileList = listOf(schemaFile1, schemaFile2)

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)

        val metricServer = mockk<IMetricServer>(relaxed = true)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)
        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.keyspaces[0].name).isEqualTo("testkeyspace1")
        assertThat(result.isSchemaInAgreement).isTrue()
    }

    @Test
    fun createSchemaTestFullSchemaFileInAgreement() {
        // test no schema files but full schema files in agreement
        val schemaFile1 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.1/schema1").path)
        val schemaFile2 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.2/schema1").path)

        val schemaFileList = emptyList<File>()

        val fullSchemaFileList = listOf(schemaFile1, schemaFile2)
        val schemasAtRoot = mockk<File>(relaxed = true)

        val metricServer = mockk<IMetricServer>(relaxed = true)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)
        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.keyspaces[0].name).isEqualTo("testkeyspace1")
        assertThat(result.isSchemaInAgreement).isTrue()
    }

    @Test
    fun createSchemaTestNoSchemaFilesExceptRoot() {
        // test no schema / full schema but the root exists
        val schemaFileList = emptyList<File>()
        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = File(this.javaClass.getResource("/fileLoaders/parsers/schema/root-schema").path)
        val metricServer = mockk<IMetricServer>(relaxed = true)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)
        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.keyspaces[0].name).isEqualTo("testkeyspace1")
        assertThat(result.isSchemaInAgreement).isTrue()
    }

    @Test
    fun createSchemaTestSchemaDisagreementWithRoot() {
        // test disagreement of schema with a root file
        val schemaFile1 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.1/schema1").path)
        val schemaFile2 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.2/schema2").path)

        val schemaFileList = listOf(schemaFile1, schemaFile2)

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = File(this.javaClass.getResource("/fileLoaders/parsers/schema/root-schema").path)

        val metricServer = mockk<IMetricServer>(relaxed = true)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)
        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.keyspaces[0].name).isEqualTo("testkeyspace1")
        assertThat(result.isSchemaInAgreement).isFalse()
    }

    @Test
    fun createSchemaTestSchemaDisagreementWithNoRoot() {
        // test disagreement of schema with no root - uses most common
        val schemaFile1 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.1/schema1").path)
        val schemaFile2 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.2/schema2").path)
        val schemaFile3 = File(this.javaClass.getResource("/fileLoaders/parsers/schema/10.0.0.3/schema2").path)

        val schemaFileList = listOf(schemaFile1, schemaFile2, schemaFile3)

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)

        val metricServer = mockk<IMetricServer>(relaxed = true)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)
        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.keyspaces[0].name).isEqualTo("testkeyspace2")
        assertThat(result.isSchemaInAgreement).isFalse()
    }

    @Test
    fun createSchemaWindowsLineEndings() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val windowsCrLfSchemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema_crlf.txt").path)
        val schemaFileListCr = listOf(windowsCrLfSchemaFile)

        val resultCrLf = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileListCr, fullSchemaFileList, schemasAtRoot)

        val windowsLfSchemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema_lf.txt").path)
        val schemaFileListLf = listOf(windowsLfSchemaFile)

        val resultLf = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileListLf, fullSchemaFileList, schemasAtRoot)

        assertThat(resultCrLf.keyspaces.size).isEqualTo(resultLf.keyspaces.size)
        assertThat(resultCrLf.tables.filter { it.isMV == false && it.is2i == false }.size).isEqualTo(resultLf.tables.filter { it.isMV == false && it.is2i == false }.size)
        assertThat(resultCrLf.types.size).isEqualTo(resultLf.types.size)
        assertThat(resultCrLf.secondaryIndexes.size).isEqualTo(resultLf.secondaryIndexes.size)
        assertThat(resultCrLf.tables.filter { it.isMV == true }.size).isEqualTo(resultLf.tables.filter { it.isMV == true }.size)
    }

    @Test
    fun testTypeExclusionForSystemTables() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val windowsLfSchemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema_dse_types.txt").path)
        val schemaFileListLf = listOf(windowsLfSchemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileListLf, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(2)
        assertThat(result.types.size).isEqualTo(3)
        assertThat(result.getUserTypes().size).isEqualTo(1)
        assertThat(result.getUserTypes()[0].contains("bar")).isTrue()
    }

    @Test
    fun testParsingMV() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-mv.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(3)
        assertThat(result.getTable("test.select_star_mv")?.name).isEqualTo("test.select_star_mv")
        assertThat(result.getTable("test.select_star_mv")?.isMV).isTrue
        assertThat(result.getTable("test.select_star_mv")?.fields?.size).isEqualTo(6)
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("id")?.dataTypeString).isEqualTo("timeuuid")
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("itemid")?.dataTypeString).isEqualTo("text")
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("createdate")?.dataTypeString).isEqualTo("timestamp")
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("createdby")?.dataTypeString).isEqualTo("text")
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("interaction")?.dataTypeString).isEqualTo("timeuuid")
        assertThat(result.getTable("test.select_star_mv")?.fields?.get("lastmodifiedate")?.dataTypeString).isEqualTo("timestamp")

        assertThat(result.getTable("test.select_mv")?.name).isEqualTo("test.select_mv")
        assertThat(result.getTable("test.select_mv")?.isMV).isTrue
        assertThat(result.getTable("test.select_mv")?.fields?.size).isEqualTo(4)
        assertThat(result.getTable("test.select_mv")?.fields?.get("id")?.dataTypeString).isEqualTo("timeuuid")
        assertThat(result.getTable("test.select_mv")?.fields?.get("itemid")?.dataTypeString).isEqualTo("text")
        assertThat(result.getTable("test.select_mv")?.fields?.get("createdate")?.dataTypeString).isEqualTo("timestamp")
        assertThat(result.getTable("test.select_mv")?.fields?.get("createdby")?.dataTypeString).isEqualTo("text")

    }

    @Test
    fun testParsingFunction() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-udf.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(0)
        assertThat(result.functions.size).isEqualTo(1)
        assertThat(result.functions.entries.first().key).isEqualTo("test.flog")

    }

    @Test
    fun testParsingSai() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-sai.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(1)
        assertThat(result.saiIndexes.size).isEqualTo(1)
    }

    @Test
    fun testParsingSasi() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-sasi.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(1)
        assertThat(result.sasiIndexes.size).isEqualTo(1)
        assertThat(result.sasiIndexes[0].baseTableName).isEqualTo("cycling.cyclist_semi_pro")
    }

    @Test
    fun testParsingSolr() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-solr.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(1)
        assertThat(result.searchIndexes.size).isEqualTo(1)
        assertThat(result.searchIndexes[0].baseTableName).isEqualTo("cycling.cyclist_semi_pro")
    }


    @Test
    fun testParsingType() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-type.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(2)
        assertThat(result.types.size).isEqualTo(1)
        assertThat(result.types[0].fields.size).isEqualTo(4)
        assertThat(result.types[0].fields.containsKey("birthday")).isTrue
        assertThat(result.types[0].fields.containsKey("nationality")).isTrue
        assertThat(result.types[0].fields.containsKey("weight")).isTrue
        assertThat(result.types[0].fields.containsKey("height")).isTrue
        assertThat(result.types[0].fields.get("birthday")).isEqualTo("timestamp")
        assertThat(result.types[0].fields.get("nationality")).isEqualTo("text")
        assertThat(result.types[0].fields.get("weight")).isEqualTo("text")
        assertThat(result.types[0].fields.get("height")).isEqualTo("text")
        assertThat(result.tables[0].hasNonFrozenTypes()).isFalse
        assertThat(result.tables[0].name).isEqualTo("cycling.cyclist_semi_pro_frozen")
        assertThat(result.tables[0].fields.getValue("info").dataTypeString).isEqualTo("frozen<cycling.basic_info>")
        assertThat(result.tables[1].hasNonFrozenTypes()).isTrue
        assertThat(result.tables[1].name).isEqualTo("cycling.cyclist_semi_pro_nonfrozen")
        assertThat(result.tables[1].fields.getValue("info").dataTypeString).isEqualTo("cycling.basic_info")
    }


    @Test
    fun testParsingCompressionDisabledTable() {
        val schemaTxt = "CREATE KEYSPACE soylent WITH replication = {'class': 'NetworkTopologyStrategy', 'bk-eu-central1-a': '3', 'bk-eu-west1-a': '3', 'bk-eu-west2-a': '3'}  AND durable_writes = true;\n" +
                "\n" +
                "CREATE TABLE soylent.bks_blob_storage (\n" +
                "    key string,\n" +
                "    chunk_id int,\n" +
                "    chunk_count int,\n" +
                "    chunk_hash ascii,\n" +
                "    total_hash ascii,\n" +
                "    value blob,\n" +
                "    PRIMARY KEY ((key, chunk_id))\n" +
                ") WITH bloom_filter_fp_chance = 0.1\n" +
                "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                "    AND comment = ''\n" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}\n" +
                "    AND compression = {'enabled': 'false'}\n" +
                "    AND crc_check_chance = 1.0\n" +
                "    AND dclocal_read_repair_chance = 0.0\n" +
                "    AND default_time_to_live = 0\n" +
                "    AND gc_grace_seconds = 864000\n" +
                "    AND max_index_interval = 256\n" +
                "    AND memtable_flush_period_in_ms = 0\n" +
                "    AND min_index_interval = 32\n" +
                "    AND read_repair_chance = 0.0\n" +
                "    AND speculative_retry = '99PERCENTILE';"
        val version = DatabaseVersion.fromString ("3.11.9")
        val schema = SchemaParser.parse(schemaTxt, metricsDb, version, true)
        assertThat(schema.keyspaces.size).isEqualTo(1)
        assertThat(schema.tables.size).isEqualTo(1)
    }
    @Test
    fun testParsingCounter() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-counters.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(1)
        assertThat(result.tables[0].fields["objects"]?.dataTypeString).isEqualTo("counter")
        assertThat(result.tables[0].fields["size"]?.dataTypeString).isEqualTo("counter")
    }

    @Test
    fun testParsingThrift() {

        val fullSchemaFileList = emptyList<File>()
        val schemasAtRoot = mockk<File>(relaxed = true)
        val schemaVersionMap = mapOf("10.0.0.1" to "abc")
        val metricServer = mockk<IMetricServer>(relaxed = true)
        every { metricServer.getSchemaVersions() } returns schemaVersionMap

        val schemaFile = File(this.javaClass.getResource("/fileLoaders/parsers/schema/schema-with-thrift.cql").path)
        val schemaFileList = listOf(schemaFile)

        val result = SchemaParser.createSchema(DatabaseVersion.latest311(), metricServer, schemaFileList, fullSchemaFileList, schemasAtRoot)

        assertThat(result.keyspaces.size).isEqualTo(1)
        assertThat(result.tables.size).isEqualTo(1)
        assertNotNull(result.getTable("\"Thrift\".\"ExampleTable1\""))
    }
}