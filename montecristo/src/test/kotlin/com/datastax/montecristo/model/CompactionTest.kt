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

import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable
import org.junit.Test
import kotlin.test.assertEquals

class CompactionTest {
    @Test
    fun testTwcsCompactionExtractedCorrectly() {
        val tableFullClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
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

        val parsedTableFullClass = ParsedCreateTable.fromCQL(tableFullClass)

        val tableShortClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'TimeWindowCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableShortClass = ParsedCreateTable.fromCQL(tableShortClass)

        assertEquals("TWCS", parsedTableFullClass.compaction.shortName)
        assertEquals("TWCS", parsedTableShortClass.compaction.shortName)
    }

    @Test
    fun testStcsCompactionExtractedCorrectly() {
        val tableFullClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
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

        val parsedTableFullClass = ParsedCreateTable.fromCQL(tableFullClass)

        val tableShortClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableShortClass = ParsedCreateTable.fromCQL(tableShortClass)

        assertEquals("STCS", parsedTableFullClass.compaction.shortName)
        assertEquals("STCS", parsedTableShortClass.compaction.shortName)
    }

    @Test
    fun testLcsCompactionExtractedCorrectly() {
        val tableFullClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableFullClass = ParsedCreateTable.fromCQL(tableFullClass)

        val tableShortClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableShortClass = ParsedCreateTable.fromCQL(tableShortClass)

        assertEquals("LCS", parsedTableFullClass.compaction.shortName)
        assertEquals("LCS", parsedTableShortClass.compaction.shortName)
    }

    @Test
    fun testDtcsCompactionExtractedCorrectly() {
        val tableFullClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableFullClass = ParsedCreateTable.fromCQL(tableFullClass)

        val tableShortClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'DateTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
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

        val parsedTableShortClass = ParsedCreateTable.fromCQL(tableShortClass)

        assertEquals("DTCS", parsedTableFullClass.compaction.shortName)
        assertEquals("DTCS", parsedTableShortClass.compaction.shortName)
    }

    @Test
    fun testLcsCompactionOptions() {
        val tableFullClass = """CREATE TABLE us_2a_scan.scan (
                                container_uuid text,
                                scan_uuid text,
                                batch_uuid text,
                                results_blob_path text,
                                PRIMARY KEY ((container_uuid, scan_uuid), batch_uuid)
                            ) WITH CLUSTERING ORDER BY (batch_uuid ASC)
                                AND bloom_filter_fp_chance = 0.01
                                AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                                AND comment = ''
                                AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'sstable_size_in_mb': '200', 'fanout_size' : '20' }
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

        val parsedTableFullClass = ParsedCreateTable.fromCQL(tableFullClass)

        assertEquals("LCS", parsedTableFullClass.compaction.shortName)
        assertEquals("200", parsedTableFullClass.compaction.getOption("sstable_size_in_mb", "160"))
    }

}