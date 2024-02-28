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

internal class GossipLogPausesWarningsMessageTest() {

    @Test
    fun testRegexParse() {
        val logEntry = LogEntry("WARN", "FailureDetector.java:288 - Not marking nodes down due to local pause of 7994062116 > 5000000000", "20201030020005", "test_node")
        val parsedEntry = GossipLogPauseMessage.fromLogEntry(logEntry)
        assertThat(parsedEntry?.host).isEqualTo("test_node")
        assertThat(parsedEntry?.pauseTimeInMs).isEqualTo(7994062116 / 1_000_000) // ns to ms
    }
}