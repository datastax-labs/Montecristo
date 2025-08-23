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

class LogRegexTest {

    @Test
    fun testDefaultPatternShortCircuitsToDefaultRegex() {
        // Given the canonical logback pattern
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        // Then the default group mapping is used (level=1, date=2, message=3)
        assertThat(regex.groupMappings[LogEntryGroupings.LEVEL]).isEqualTo(1)
        assertThat(regex.groupMappings[LogEntryGroupings.DATE]).isEqualTo(2)
        assertThat(regex.groupMappings[LogEntryGroupings.MESSAGE]).isEqualTo(3)
    }

    @Test
    fun testConvertKeepsGroupOrderForStandardLayout() {
        // Given a non-default pattern that still lists level, date then message
        val regex = LogRegex.convert("%-5level [%thread] %date{yyyy-MM-dd HH:mm:ss.SSS} %F:%L - %msg%n")
        // Then the group positions reflect that order, skipping the non-capturing fraction group
        assertThat(regex.groupMappings[LogEntryGroupings.LEVEL]).isEqualTo(1)
        assertThat(regex.groupMappings[LogEntryGroupings.DATE]).isEqualTo(2)
        assertThat(regex.groupMappings[LogEntryGroupings.MESSAGE]).isEqualTo(3)
    }

    @Test
    fun testConvertTracksReorderedGroups() {
        // Given a pattern where the date comes before the level
        val regex = LogRegex.convert("%date{ISO8601} %-5level [%thread] %F:%L - %msg%n")
        // Then the computed positions follow the new ordering
        assertThat(regex.groupMappings[LogEntryGroupings.DATE]).isEqualTo(1)
        assertThat(regex.groupMappings[LogEntryGroupings.LEVEL]).isEqualTo(2)
        assertThat(regex.groupMappings[LogEntryGroupings.MESSAGE]).isEqualTo(3)
    }

    @Test
    fun testConvertedRegexMatchesCommaMillisLine() {
        // Given a converted regex from a reordered pattern
        val regex = LogRegex.convert("%date{ISO8601} %-5level [%thread] %F:%L - %msg%n")
        val line = "2020-09-03 10:25:03,316 WARN [SharedPool-1] ReadCommand.java:520 - Read 61 live rows"
        // Then it matches the corresponding log line
        assertThat(regex.regex.matches(line)).isTrue()
    }

    @Test
    fun testConvertedRegexMatchesDottedMillisLine() {
        // Given a pattern whose date format separates sub-seconds with a '.'
        val regex = LogRegex.convert("%-5level [%thread] %date{yyyy-MM-dd HH:mm:ss.SSS} %F:%L - %msg%n")
        val line = "INFO [GossipStage:1] 2022-06-01 08:15:30.742 Gossiper.java:101 - node up"
        // Then the line matches
        assertThat(regex.regex.matches(line)).isTrue()
    }
}
