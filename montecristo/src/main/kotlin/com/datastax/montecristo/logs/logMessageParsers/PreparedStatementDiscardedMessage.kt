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
import java.time.LocalDateTime

data class PreparedStatementDiscardedMessage(
    val date: LocalDateTime,
    val host: String
) {
    // WARN  [ScheduledTasks:1] 2022-08-28 23:22:09,805  QueryProcessor.java:148 - 179 prepared statements discarded in the last minute because cache limit reached (125 MB)
    companion object {
        private val regexPreparedStatements = "(\\d*) .*".toRegex() // $ needed to prevent it matching as a substring

        fun fromLogEntry(entry: LogEntry): PreparedStatementDiscardedMessage? {
            val message = entry.message!!
            val result = regexPreparedStatements.find(message)

            if (result != null) {
                return PreparedStatementDiscardedMessage(
                    entry.getDate(),
                    entry.host ?: "unknown node"
                )
            } else {
                // unable to parse the message
            }
            return null
        }
    }
}