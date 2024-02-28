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

import com.datastax.montecristo.helpers.FileHelpers
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.logs.LogbackAppender
import java.io.File

object LogFileFinder {

    // Get a log config file for a single node.
    fun getLogbackConfig(artefactsDir: File): List<String> {
        val logbackFileList = FileHelpers.getFilesWithName(File(artefactsDir, "conf").absolutePath, "logback.xml")
        return if (logbackFileList.isNotEmpty()) {
            logbackFileList.first().readLines()
        } else {
            emptyList()
        }
    }

    // Use the logback.xml settings and some well known log file names to find log files.
    fun getLogFiles(artifactsDirectory: File): Pair<List<File>, LogbackAppender> {
        val logbackConfig = getLogbackConfig(artifactsDirectory)
        val logSettings = LogSettingsParser.parseLoggers(logbackConfig)
        val logbackLogFileAppenderUsed = logSettings.appenders
            .asSequence()
            .filter { it.appenderClass == "ch.qos.logback.core.rolling.RollingFileAppender" } // Only log file appenders.
            .filter { it.maxLevel <= LogLevel.WARN } // Only loglevels of WARN or less (we rely on mostly WARN level messages but not INFO or DEBUG level.)
            .sortedByDescending { it.maxLevel } // Sort by Logging level, pick up highest level logger on the basis that we assume it has the most history (depending on the rolling strategy).
            .filter { it.filePatterns.isNotEmpty() }
            .firstOrNull() ?: LogbackAppender.defaultSystemLog()
        val logbackLogFile = logbackLogFileAppenderUsed
            .filePatterns
            .firstOrNull() // Take the first one.
            ?.split("/")?.last() // Just the filename
            ?.replace(".", "\\.") // Make regex ready.
        val logbackLogFiles = FileHelpers.getFilesWithName(File(artifactsDirectory, "logs").absolutePath, "$logbackLogFile.*")

        val logFiles = logbackLogFiles.ifEmpty {
            // no files found, try again with 'well known' log file names
            FileHelpers.getFilesWithName(
                File(artifactsDirectory, "logs").absolutePath,
                "application\\.log.*|system\\.log.*|stdout.*|cassandra_0\\.log.*"
            )
        }
        return Pair(logFiles, logbackLogFileAppenderUsed)
    }
}