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

package com.datastax.montecristo.logs.logMessageParsers

import com.datastax.montecristo.logs.LogEntry
import org.junit.Assert.assertEquals
import org.junit.Test

internal class TombstoneWarningMessageTest() {

    @Test
    fun testRegexParse() {
        val logEntry = LogEntry(
            "WARN",
            "Read 2000 live rows and 1238 tombstone cells for query SELECT * FROM test_keyspace.test_table",
            "20241028000000",
            "test_node"
        )
        val parsedEntry = TombstoneWarningMessage.fromLogEntry(logEntry)
        assertEquals(1238L, parsedEntry?.tombstoneRows)
        assertEquals(2000L, parsedEntry?.liveRows)
        assertEquals("test_node", parsedEntry?.host)
        assertEquals("test_keyspace.test_table", parsedEntry?.tableName)
    }

    @Test
    fun testRegexParseWithWhere() {
        val logEntry = LogEntry(
            "WARN",
            "Read 2000 live rows and 1238 tombstone cells for query SELECT * FROM test_keyspace.test_table WHERE foo=bar",
            "20241028000000",
            "test_node"
        )
        val parsedEntry = TombstoneWarningMessage.fromLogEntry(logEntry)
        assertEquals(1238L, parsedEntry?.tombstoneRows)
        assertEquals(2000L, parsedEntry?.liveRows)
        assertEquals("test_node", parsedEntry?.host)
        assertEquals("test_keyspace.test_table", parsedEntry?.tableName)
    }

    @Test
    fun testRegexParseWithLimit() {
        val logEntry = LogEntry(
            "WARN",
            "Read 2000 live rows and 1238 tombstone cells for query SELECT * FROM test_keyspace.test_table LIMIT 2000 ALLOW FILTERING; token -9141048252213628504 (see tombstone_warn_threshold)",
            "20241028000000",
            "test_node"
        )
        val parsedEntry = TombstoneWarningMessage.fromLogEntry(logEntry)
        assertEquals(1238L, parsedEntry?.tombstoneRows)
        assertEquals(2000L, parsedEntry?.liveRows)
        assertEquals("test_node", parsedEntry?.host)
        assertEquals("test_keyspace.test_table", parsedEntry?.tableName)
    }

}