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

package com.datastax.montecristo.model

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import com.datastax.montecristo.fileLoaders.parsers.schema.ClusteringKey
import com.datastax.montecristo.fileLoaders.parsers.schema.Direction
import com.datastax.montecristo.fileLoaders.parsers.schema.Field
import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable
import com.datastax.montecristo.model.schema.CompactionDetail
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertEquals


class ParsedCreateTableTest {
    val createTable = """CREATE TABLE mytable (id int,
                        |cluster timeuuid,
                        |another_cluster timeuuid,
                        |val text,
                        |another map<int, int>,
                        |primary key(id, cluster, another_cluster))
                        |WITH CLUSTERING
                        |ORDER BY (cluster DESC,
                                   another_cluster ASC)""".trimMargin()

    val createView = """ """.trimMargin()

    val createindex = """ """.trimMargin()

    @Test
    fun testFieldParsing() {

        val parsed = ParsedCreateTable.fromCQL(createTable)
        assertEquals("id", parsed.partitionKeys.first())
        assertEquals("cluster", parsed.clusteringKeys.first().first)
        assertEquals(2, parsed.clusteringKeys.size)
        assertEquals(5, parsed.fields.size)
    }

    @Test
    fun test20Parsing() {
        val cql = """CREATE TABLE text_feature (
  input_hash text,
  outputs list<blob>,
  PRIMARY KEY ((input_hash))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};"""
        val parsed = ParsedCreateTable.fromCQL(cql)
        assertEquals(parsed.name, "text_feature")
        assertEquals("ALL", parsed.caching.keys)
        assertEquals("NONE", parsed.caching.rows)
    }

    // i'm pretty sure this is 3.anything
    @Test
    fun test311Parsing() {
        val schema = """CREATE TABLE system_distributed.parent_repair_history (
    parent_id timeuuid PRIMARY KEY,
    columnfamily_names set<text>,
    exception_message text,
    exception_stacktrace text,
    finished_at timestamp,
    keyspace_name text,
    options map<text, text>,
    requested_ranges set<text>,
    started_at timestamp,
    successful_ranges set<text>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'Repair history'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99p';
"""
        val parsed = ParsedCreateTable.fromCQL(schema)
        assertEquals("ALL", parsed.caching.keys)
    }

    @Test
    fun test311MvParsing() {
        val schema = """CREATE MATERIALIZED VIEW customer_portal.bycity AS
    SELECT *
    FROM customer_portal.addresses
    WHERE localitelc IS NOT NULL AND ruelc IS NOT NULL AND numero IS NOT NULL
    PRIMARY KEY (localitelc, ruelc, code_postal, numero)
    WITH CLUSTERING ORDER BY (ruelc ASC, code_postal ASC, numero ASC)
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
    AND speculative_retry = '99PERCENTILE';
"""
        val parsed = ParsedCreateTable.fromCQL(schema)
        assertEquals("ALL", parsed.caching.keys)
        assertEquals(ImmutableList.of("localitelc"), parsed.partitionKeys)

        val expectedClusteringKeys = ImmutableList.of(
                ClusteringKey("ruelc", Direction.ASC),
                ClusteringKey("code_postal", Direction.ASC),
                ClusteringKey("numero", Direction.ASC)
        )
        assertEquals(expectedClusteringKeys, parsed.clusteringKeys)

        assertEquals(ImmutableMap.of(), parsed.fields)
        assertEquals(864000, parsed.gcGraceSeconds)
    }

    @Test
    fun testParsingWithQuotes() {
        val schema = """CREATE TABLE "Monitoring"."CmcOpLog" (
    key text,
    column1 text,
    value text,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.01
    AND speculative_retry = 'NONE';"""
        val parsed = ParsedCreateTable.fromCQL(schema)
        assertThat(parsed.name).isEqualTo("CmcOpLog")

    }


    @Test
    fun testMvParsingWithActualSelectClause() {
        val schema = "CREATE MATERIALIZED VIEW pingid.sessions_by_user AS     " +
                "SELECT org_user_id, start_time, completion_status, sp_alias, session_id, session_type, status_reason, user_retries     " +
                "FROM pingid.sessions     " +
                "WHERE org_user_id IS NOT NULL AND sp_alias IS NOT NULL AND start_time IS NOT NULL AND completion_status IS NOT NULL AND session_id IS NOT NULL     " +
                "PRIMARY KEY (org_user_id, start_time, completion_status, sp_alias, session_id)     " +
                "WITH CLUSTERING ORDER BY (start_time DESC, completion_status ASC, sp_alias ASC, session_id ASC)     " +
                "AND bloom_filter_fp_chance = 0.01     " +
                "AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}     " +
                "AND comment = ''    " +
                "AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}     " +
                "AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}     " +
                "AND crc_check_chance = 1.0     " +
                "AND dclocal_read_repair_chance = 0.1     " +
                "AND default_time_to_live = 0     " +
                "AND gc_grace_seconds = 864000     " +
                "AND max_index_interval = 2048     " +
                "AND memtable_flush_period_in_ms = 0     " +
                "AND min_index_interval = 128     " +
                "AND read_repair_chance = 0.0     " +
                "AND speculative_retry = '99PERCENTILE'"

        val parsed = ParsedCreateTable.fromCQL(schema)
        assertEquals("ALL", parsed.caching.keys)
        assertEquals(ImmutableList.of("org_user_id"), parsed.partitionKeys)

        val expectedClusteringKeys = ImmutableList.of(
                ClusteringKey("start_time", Direction.ASC),
                ClusteringKey("completion_status", Direction.ASC),
                ClusteringKey("sp_alias", Direction.ASC),
                ClusteringKey("session_id", Direction.ASC)
        )
        assertEquals(expectedClusteringKeys, parsed.clusteringKeys)

        val expectedFields : HashMap<String, Field> = Maps.newHashMap()
        expectedFields.put("org_user_id", Field("UNKNOWN_TYPE"))
        expectedFields.put("start_time", Field("UNKNOWN_TYPE"))
        expectedFields.put("completion_status", Field("UNKNOWN_TYPE"))
        expectedFields.put("sp_alias", Field("UNKNOWN_TYPE"))
        expectedFields.put("session_id", Field("UNKNOWN_TYPE"))
        expectedFields.put("session_type", Field("UNKNOWN_TYPE"))
        expectedFields.put("status_reason", Field("UNKNOWN_TYPE"))
        expectedFields.put("user_retries", Field("UNKNOWN_TYPE"))

        assertEquals(expectedFields, parsed.fields)
        assertEquals(864000, parsed.gcGraceSeconds)

    }

    @Test
    fun testParsingSecondaryIndex() {
        val schema = "CREATE INDEX my_idx ON my_ks.my_table (my_column);"

        val parsed = ParsedCreateTable.fromCQL(schema)
        assertEquals("my_idx", parsed.indexName)
        assertEquals("my_ks.my_table", parsed.name)
        assertEquals(ImmutableList.of("my_column"), parsed.partitionKeys)
    }

    @Test
    fun testParsingCFSStrategy() {
        val schema = "CREATE TABLE cfs_archive.sblocks (\n" +
                "    key blob,\n" +
                "    column1 blob,\n" +
                "    value blob,\n" +
                "    PRIMARY KEY (key, column1)\n" +
                ") WITH COMPACT STORAGE\n" +
                "    AND CLUSTERING ORDER BY (column1 ASC)\n" +
                "    AND read_repair_chance = 0.0\n" +
                "    AND dclocal_read_repair_chance = 0.1\n" +
                "    AND gc_grace_seconds = 864000\n" +
                "    AND bloom_filter_fp_chance = 6.8E-5\n" +
                "    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\n" +
                "    AND comment = 'Stores blocks of information associated with a inode'\n" +
                "    AND compaction = { 'class' : 'com.datastax.bdp.hadoop.cfs.compaction.CFSCompactionStrategy', 'max_threshold' : 64, 'min_threshold' : 2 }\n" +
                "    AND compression = { 'enabled' : 'false' }\n" +
                "    AND default_time_to_live = 0\n" +
                "    AND speculative_retry = 'NONE'\n" +
                "    AND min_index_interval = 128\n" +
                "    AND max_index_interval = 2048\n" +
                "    AND crc_check_chance = 1.0\n" +
                "    AND cdc = false;"
        val parsed = ParsedCreateTable.fromCQL(schema)
        assertThat(parsed.name).isEqualTo("sblocks")
        assertThat(parsed.compaction.shortName).isEqualTo("CFS")

    }
}