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

data class CommitLogSyncMessage(val totalCommits: Int, val messageInterval: Double, val averageDuration: Double, val numberExceeding: Int, val averageExceededCommitInterval: Double, val date: LocalDateTime, val host: String) {

    companion object {
        val regex = "Out of (\\d+) commit log syncs over the past (\\d+.\\d+|\\d+)s with average duration of (\\d+.\\d+|\\d+)ms, (\\d+) have exceeded the configured commit interval by an average of (\\d+.\\d+|\\d+)ms".toRegex()

        fun fromLogEntry(entry: LogEntry): CommitLogSyncMessage? {
            val message = entry.message?: ""
            val result = regex.find(message)

            if (result != null) {
                return CommitLogSyncMessage(result.groupValues[1].toInt()
                        , result.groupValues[2].toDouble()
                        , result.groupValues[3].toDouble()
                        , result.groupValues[4].toInt()
                        , result.groupValues[5].toDouble()
                        , entry.getDate()
                        , entry.host?:"unknown node")
            } else {
                // unable to parse the message
                // the most likely scenario is the Infinityms bug : https://issues.apache.org/jira/browse/CASSANDRA-14451
            }
            return null
        }
    }
}