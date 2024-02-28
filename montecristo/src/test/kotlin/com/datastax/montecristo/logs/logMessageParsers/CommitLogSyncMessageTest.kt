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
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class CommitLogSyncMessageTest() {

    @Test
    fun testRegexParse() {

        val logEntry = LogEntry("WARN", "Out of 1 commit log syncs over the past 0.00s with average duration of 3168.68ms, 11 have exceeded the configured commit interval by an average of 2168.68ms", "20201030020005", "test_node")
        val parsedEntry = CommitLogSyncMessage.fromLogEntry(logEntry)
        assertThat(parsedEntry?.host).isEqualTo("test_node")
        assertThat(parsedEntry?.totalCommits).isEqualTo(1)
        assertThat(parsedEntry?.messageInterval).isEqualTo(0.0)
        assertThat(parsedEntry?.averageDuration).isEqualTo(3168.68)
        assertThat(parsedEntry?.numberExceeding).isEqualTo(11)
        assertThat(parsedEntry?.averageExceededCommitInterval).isEqualTo(2168.68)

    }
}