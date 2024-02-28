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

package com.datastax.montecristo.fileLoaders.parsers.statistics

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.File
import kotlin.test.assertEquals

internal class SSTableStatisticsParserTest {

    @Test
    fun testExtractKeyspaceTableFromFile() {
        val ksTbl = SSTableStatisticsParser.extractKeyspaceTableFromFile(File(this.javaClass.getResource("/fileLoaders/parsers/sstable-statistics/keyspace1/table2-uuidABCD/bb-1-bti-Statistics.db").path))
        assertEquals(ksTbl, "keyspace1.table2")
    }

    @Test
    fun testParseOSS_md() {
        val result = SSTableStatisticsParser.parse(listOf (File(this.javaClass.getResource("/fileLoaders/parsers/sstable-statistics/keyspace1/table1/md-1-big-Statistics.db").path)))
        assertThat(result.data.size).isEqualTo(1)
        assertThat(result.data.first().rowCount).isEqualTo(4)
        assertThat(result.data.first().keyspaceTableName).isEqualTo("keyspace1.table1")
    }

    @Test
    fun testParseOSS_txt() {
        val result = SSTableStatisticsParser.parse(listOf (File(this.javaClass.getResource("/fileLoaders/parsers/sstable-statistics/keyspace1/table1/md-1-big-Statistics.txt").path)))
        assertThat(result.data.size).isEqualTo(1)
        assertThat(result.data.first().rowCount).isEqualTo(4)
        assertThat(result.data.first().keyspaceTableName).isEqualTo("keyspace1.table1")
    }
}