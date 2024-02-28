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

data class LogRegex(val regex: Regex, val groupMappings: Map<LogEntryGroupings, Int>) {

    companion object {

        fun defaultRegex() : LogRegex {
            val defaultRegex = Regex("""^\s*(\w+)\s*\[.*?]\s([^,]*),\d*(.*)""", RegexOption.DOT_MATCHES_ALL)
            val defaultMapping = mapOf(
                LogEntryGroupings.LEVEL to 1,
                LogEntryGroupings.DATE to 2,
                LogEntryGroupings.MESSAGE to 3
            )
            return (LogRegex(defaultRegex, defaultMapping))
        }

        fun convert(logbackPattern: String): LogRegex {
            val defaultLogbackPattern = "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"


            // Default Pattern
            // <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
            if (logbackPattern == defaultLogbackPattern) {
                // the bulk of the cases do not need more complex logic, return the default
                // regex and group mapping
                return defaultRegex()
            } else {
                // replace the markers with the regex's for the section
                // %-5Level == \s*(\w+)\s*
                // [%thread] == \s*\[.*?]\s*
                // %date{ISO8601} == \s*([^,]*),\d*\s*
                // %X{service} == .*\[.*?]
                // %F:%L  == class / line, included in the message
                // %msg%n == (.*)
                val potentialRegex = logbackPattern
                    .replace("%-5level", "(\\w+)")
                    .replace("[%thread]", "\\[.*?]")
                    .replace("%date{ISO8601}", "([^,]*),\\d*")
                    .replace("%X{service}", "" ) // becomes a part of the message
                    .replace("%F:%L - %msg%n", "(.*)")
                    .replace("%F:%L %M %msg%n", "(.*)")
                    .replace("%marker", "") // becomes part of the message
                    .replace(" ","\\s*")

                // now need to understand, the order of the groupings
                val levelPosition = potentialRegex.substringBefore("(\\w+)").filter { it == '('}.count() + 1
                val datePosition = potentialRegex.substringBefore("([^,]*),\\d*").filter { it == '('}.count() + 1
                val messagePosition = potentialRegex.substringBefore( "(.*)").filter { it == '('}.count() + 1
                val groupMappings = mapOf(
                    LogEntryGroupings.LEVEL to levelPosition,
                    LogEntryGroupings.DATE to datePosition,
                    LogEntryGroupings.MESSAGE to messagePosition
                )
                val regex = Regex(potentialRegex, RegexOption.DOT_MATCHES_ALL)
                return LogRegex(regex, groupMappings)
            }
        }
    }
}