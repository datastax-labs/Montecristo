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

data class TombstoneWarningMessage(val liveRows: Long, val tombstoneRows: Long, val tableName: String, val date: LocalDateTime, val host: String) {

    companion object {
        val regex = ".*Read (\\d+) live rows and (\\d+) tombstone cells for query.*FROM (.*) WHERE|LIMIT .*".toRegex()
        fun fromLogEntry(entry: LogEntry): TombstoneWarningMessage? {
            val message = entry.message!!
            val result = regex.find(message)

            if (result != null) {
                return TombstoneWarningMessage(result.groupValues[1].toLong()
                    , result.groupValues[2].toLong()
                    , result.groupValues[3]
                    , entry.getDate()
                    , entry.host?:"unknown node")
            } else {
                // unable to parse the message
            }
            return null
        }
    }
}