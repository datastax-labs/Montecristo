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

data class GossipLogPauseMessage (val pauseTimeInMs: Long, val date: LocalDateTime, val host: String) {

    companion object {
        // first number is the line number of the FailureDetector class, 2nd number is the pause duration, number 3 is the threshold (default 5 seconds in nanoseconds)
        val regex = "FailureDetector\\.java:(\\d+) - Not marking nodes down due to local pause of (\\d+) > (\\d+)".toRegex()

        fun fromLogEntry(entry: LogEntry): GossipLogPauseMessage? {
            val message = entry.message?:""
            val result = regex.find(message)

            if (result != null) {
                return GossipLogPauseMessage(result.groupValues[2].toLong() / 1_000_000 // the time is in nano seconds, which is overkill.
                        , entry.getDate()
                        , entry.host?:"unknown node")
            } else {
                // unable to parse the message, discarded
            }
            return null
        }
    }
}