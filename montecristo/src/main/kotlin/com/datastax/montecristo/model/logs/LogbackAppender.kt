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

package com.datastax.montecristo.model.logs

data class LogbackAppender(val name: String,
                           val appenderClass: String,
                           val filePatterns: List<String> = emptyList(),
                           val maxLevel: LogLevel,
                           val encoderPattern : String)

{

    companion object {

        fun defaultSystemLog() : LogbackAppender {
            return LogbackAppender(
                "STDOUT",
                "ch.qos.logback.core.rolling.RollingFileAppender",
                listOf("\${cassandra.logdir}/system.log"),
                LogLevel.INFO,
                "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"
            )
        }
    }
}