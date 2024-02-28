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

class DroppedOperationMessage (val messageType : String, val internalDrops: Long, val crossNodeDrops: Long, val date: LocalDateTime, val host: String) {

    companion object {
        // INFO  [ScheduledTasks:1] 2020-11-09 12:09:37,384 MessagingService.java:1246 - MUTATION messages were dropped in last 5000 ms: 1 internal and 45 cross node. Mean internal dropped latency: 2874 ms and Mean cross-node dropped latency: 2395 ms
      //  val regex = "([A-Z]+) messages were dropped in last (\\d+) ms\\: (\\d+) internal and (\\d+) cross node\\. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency\\: (\\d+) ms".toRegex()
        val regex = "([A-Z]+) messages were dropped in (?:last|the last) (\\d+) (?:ms|s): (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms".toRegex()
        fun fromLogEntry(entry: LogEntry): DroppedOperationMessage? {
            val message = entry.message?:""
            val result = regex.find(message)

            return if (result != null) {
                return DroppedOperationMessage(result.groupValues[1]
                        , result.groupValues[3].toLong()
                        , result.groupValues[4].toLong()
                        , entry.getDate()
                        , entry.host?:"unknown node")
            } else {
                // unable to parse the message
                null
            }
        }
    }
}