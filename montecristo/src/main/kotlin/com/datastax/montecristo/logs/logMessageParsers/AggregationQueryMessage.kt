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

data class AggregationQueryMessage(
    val keyspaceTable: String,
    val isMultiplePartitionKeys: Boolean,
    val date: LocalDateTime,
    val host: String
) {

    companion object {
        private val regexGlobalAggregation = ".*Aggregation query used without partition key$".toRegex() // $ needed to prevent it matching as a substring
        private val regexGlobalAggregationWithDetails = ".*Aggregation query used without partition key \\(ks: (.*), tbl: (.*)\\)".toRegex()
        private val regexMultiPartitionAggregation = ".*Aggregation query used on multiple partition keys \\(IN restriction\\) \\(ks: (.*), tbl: (.*)\\)".toRegex()
        private val regexMultiPartitionAggregationNoTable =  ".*Aggregation query used on multiple partition keys \\(IN restriction\\)".toRegex()

        fun fromLogEntry(entry: LogEntry): AggregationQueryMessage? {
            val message = entry.message!!
            var multiPartition = false
            val result = when {
                regexGlobalAggregation.matches(message) -> {
                    regexGlobalAggregation.find(message)
                }
                regexGlobalAggregationWithDetails.matches(message) -> {
                    regexGlobalAggregationWithDetails.find(message)
                }
                regexMultiPartitionAggregation.matches(message) -> {
                    multiPartition = true
                    regexMultiPartitionAggregation.find(message)
                }
                else -> {
                    multiPartition = true
                    regexMultiPartitionAggregationNoTable.find(message)
                }
            }

            if (result != null) {
                val tableName = if (result.groupValues.size > 1) {
                    "${result.groupValues[1]}.${result.groupValues[2]}"
                } else {
                    "unknown"
                }
                // we have a ks / table combination, use it.
                return AggregationQueryMessage(
                    tableName,
                    multiPartition,
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