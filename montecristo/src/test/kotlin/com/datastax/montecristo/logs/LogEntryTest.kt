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

package com.datastax.montecristo.logs

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.time.LocalDateTime

class LogEntryTest {

    // The default logback pattern, which LogRegex.convert short-circuits to defaultRegex().
    private val defaultPattern = "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"

    @Test
    fun testFromStringParsesDefaultPattern() {
        // Given a standard Cassandra log line (ISO8601 date with comma-separated millis)
        val line =
            "WARN  [SharedPool-Worker-1] 2020-09-03 10:25:03,316 ReadCommand.java:520 - Read 61 live rows"
        // When parsing with the regex derived from the default logback pattern
        val regex = LogRegex.convert(defaultPattern)
        val entry = LogEntry.fromString(line, regex)
        // Then the level, normalised timestamp and message are extracted
        assertThat(entry.level).isEqualTo("WARN")
        assertThat(entry.timestamp).isEqualTo("20200903102503")
        assertThat(entry.message).isEqualTo("ReadCommand.java:520 - Read 61 live rows")
    }

    @Test
    fun testFromStringAcceptsTDelimiter() {
        // Given a log line whose date uses a 'T' between date and time (ISO8601 variant)
        val line =
            "ERROR [main] 2021-12-31T23:59:59,001 Startup.java:42 - boom"
        val regex = LogRegex.convert(defaultPattern)
        // When parsing
        val entry = LogEntry.fromString(line, regex)
        // Then the 'T' is normalised away and the timestamp parses correctly
        assertThat(entry.level).isEqualTo("ERROR")
        assertThat(entry.timestamp).isEqualTo("20211231235959")
    }

    @Test
    fun testFromStringAcceptsDottedMillis() {
        // Given a non-default pattern whose date format uses a '.' before the sub-second value
        val pattern = "%-5level [%thread] %date{yyyy-MM-dd HH:mm:ss.SSS} %F:%L - %msg%n"
        val line =
            "INFO  [GossipStage:1] 2022-06-01 08:15:30.742 Gossiper.java:101 - node up"
        val regex = LogRegex.convert(pattern)
        // When parsing
        val entry = LogEntry.fromString(line, regex)
        // Then both the dotted fraction and the message are handled
        assertThat(entry.level).isEqualTo("INFO")
        assertThat(entry.timestamp).isEqualTo("20220601081530")
        assertThat(entry.message).isEqualTo("Gossiper.java:101 - node up")
    }

    @Test
    fun testFromStringReturnsEmptyForBlankLine() {
        // Given a blank line
        val regex = LogRegex.convert(defaultPattern)
        // When parsing
        val entry = LogEntry.fromString("   ", regex)
        // Then an empty LogEntry is returned (no level, no timestamp)
        assertThat(entry.level).isEqualTo("")
        assertThat(entry.timestamp).isEqualTo("")
    }

    @Test
    fun testFromStringReturnsEmptyForUnparseableLine() {
        // Given a line that matches neither the primary nor the fallback regex
        val regex = LogRegex.convert(defaultPattern)
        // When parsing
        val entry = LogEntry.fromString("not a log line at all", regex)
        // Then an empty LogEntry is returned rather than throwing
        assertThat(entry.level).isEqualTo("")
        assertThat(entry.timestamp).isEqualTo("")
    }

    @Test
    fun testGetDateReturnsLocalDateTime() {
        // Given a LogEntry with an internal-format timestamp
        val entry = LogEntry("DEBUG", "dummy", "20250824123045")
        // When converting the timestamp to a LocalDateTime
        val actual = entry.getDate()
        // Then it matches the expected date/time
        assertThat(actual).isEqualTo(LocalDateTime.of(2025, 8, 24, 12, 30, 45))
    }
}
