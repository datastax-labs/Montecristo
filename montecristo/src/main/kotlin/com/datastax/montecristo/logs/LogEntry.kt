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

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class LogEntry(
    var level: String,
    var message: String?,
    var timestamp: String,
    var host: String? = null
) {

    // <pattern>%-5level [%thread] %date{ISO8601} %F:%L - %msg%n</pattern>
    companion object {

        private val alt_regex1 = Regex("""^([^,]*).*(WARN|INFO|DEBUG|ERROR|FATAL|TRACE)(.*)""", RegexOption.DOT_MATCHES_ALL)
        private const val internalFormat = "yyyyMMddHHmmss"
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(internalFormat)

        fun fromString(line: String, logRegex : LogRegex): LogEntry {

            // why do we have empty lines?
            if (line.trim() == "") {
                return LogEntry("", "", "")
            }
            try {
                if (logRegex.regex.matches(line)) {
                    val groups = logRegex.regex.find(line)?.groups
                    val level = groups?.get(logRegex.groupMappings.getOrDefault(LogEntryGroupings.LEVEL, 1))?.value ?: ""
                    val isoFormatDate = groups?.get(logRegex.groupMappings.getOrDefault(LogEntryGroupings.DATE, 2))?.value?.trim()

                    if (level == "" || isoFormatDate == "") {
                        return LogEntry("", "", "")
                    }
                    val parsedDate = SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(isoFormatDate)
                    val timestamp = SimpleDateFormat(internalFormat).format(parsedDate)

                    val message = groups?.get(logRegex.groupMappings.getOrDefault(LogEntryGroupings.MESSAGE, 3))?.value?.trim()
                    if (isoFormatDate.isNullOrBlank()) {
                        logger.info("BLANK TIMESTAMP : $line")
                    }
                    return LogEntry(level, message, timestamp)
                } else {
                    val groups = alt_regex1.find(line)?.groups

                    val level = groups?.get(2)?.value ?: ""

                    val extractedDate = groups?.get(1)?.value?.trim()

                    if (level == "" || extractedDate == "") {
                        return LogEntry("", "", "")
                    }

                    val parsedDate = SimpleDateFormat("d MMM yyyy HH:mm:ss", Locale.ENGLISH).parse(extractedDate)
                    val timestamp = SimpleDateFormat(internalFormat).format(parsedDate)

                    val message = groups?.get(3)?.value?.trim()
                    return LogEntry(level, message, timestamp)
                }
            } catch (e: Exception) {
                return LogEntry("", "", "")
            }
        }
    }

    /**
     * returns a
     */
    fun getDate(): LocalDateTime {
        return LocalDateTime.parse(timestamp, formatter)
    }

}
