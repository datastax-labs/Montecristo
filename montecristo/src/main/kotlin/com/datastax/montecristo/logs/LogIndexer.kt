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

import com.datastax.montecristo.model.logs.IndexWriterStatistics
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.Directory
import org.apache.lucene.util.BytesRef
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.time.LocalDateTime

val logger: Logger = LoggerFactory.getLogger(LogIndexer::class.java)

object LogIndexer {

    fun getWriter(directory: Directory): IndexWriter {
        val analyzer = StandardAnalyzer()
        val config = IndexWriterConfig(analyzer)
        return IndexWriter(directory, config)
    }


    fun getLogEntries(stream: InputStream, logRegex: LogRegex): List<LogEntry> {

        // if we need to find other stuff, here we go.
        val reader = BufferedReader(InputStreamReader(stream))

        val logEntryList = mutableListOf<LogEntry>()
        val current = StringBuilder("")
        val regex = "^.*(WARN|INFO|DEBUG|ERROR|FATAL|TRACE)".toRegex()

        // The complexity being solved is that following lines might belong to this line. If the next line does not match the regex
        // then it is a follow on line. The code is building up a collection of strings, but on larger log files, this creating a huge memory
        // overhead, it should process each line once it has the information it needs.
        reader.lines().forEach {
            // if it finds an entry, and the current is not empty, then that means current is the left over from the previous line, add it
            // and then start a new 'current'
            // otherwise (else) just put this line into the current. (meaning it didn't find a match, so it must be a continuation line.)
            if (regex.find(it) != null && current.isNotEmpty()) {
                logEntryList.add(LogEntry.fromString(current.toString(), logRegex))
                // The code would previously assign a new StringBuilder() object to current, the problem there however is
                // that you generate a huge number of StringBuilder objects to be GC'ed, 1 per log entry!
                current.setLength(0)
                current.append(it + "\n")
            } else {
                current.appendLine(it)
            }
        }
        // if the current had something in it at the end (the last line will appear here), then add it onto the strings.
        if (current.isNotEmpty()) {
            logEntryList.add(LogEntry.fromString(current.toString(), logRegex))
        }
        return logEntryList
    }


    fun writeToIndex(
        index: IndexWriter,
        logEntries: List<LogEntry>,
        host: String
    ): IndexWriterStatistics {

        var written = 0
        val documents = mutableListOf<Document>()

        // we need to keep a track of the minimum / maximum values of the entry dates, they are entered into the lucene as a string entry
        // so we can't search them so easy, we can keep a track of them as we index them.
        // the minimum for this file will be the first, and maximum the last - the custom iteration though prevents us using first / last.
        var minDate: LocalDateTime = LocalDateTime.MAX
        var maxDate: LocalDateTime = LocalDateTime.MIN
        var isFirst = true
        val skippedEntries = logEntries.filter { it.timestamp.isBlank() }.count()
        val validEntries = logEntries.filter { it.timestamp.isNotBlank() }
        logger.info("Skipped $skippedEntries log entries due to parsing issues")
        var errorCount = 0
        validEntries.forEach { entry ->
            try {
                if (isFirst) {
                    // process once only
                    logger.info("date = ${entry.timestamp}")
                    minDate = entry.getDate()
                    isFirst = false
                }

                val doc = Document()
                val message = entry.message ?: ""
                if (message.contains("StatusLogger.java") || message == "") {
                    logger.debug("Skipping statuslogger")
                } else {
                    doc.add(StringField("host", host, Field.Store.YES))
                    doc.add(StringField("level", entry.level, Field.Store.YES))
                    doc.add(TextField("message", entry.message, Field.Store.YES))
                    doc.add(StringField("timestamp", entry.timestamp, Field.Store.YES))
                    doc.add(SortedDocValuesField("timestamp", BytesRef(entry.timestamp)))
                    documents.add(doc)

                    if (documents.size == 1000) {
                        index.addDocuments(documents)
                        documents.clear()
                    }
                    written++
                    // keep updating this, whatever it finishes as will be the maximum
                    maxDate = entry.getDate()
                }
            } catch (ex: Exception) {
            //    logger.error("failed to parse log row : host=${host} timestamp=${entry.timestamp} level=${entry.level} message=${entry.message}")
                errorCount++
            }
        }

        index.addDocuments(documents)
        logger.info("$written logs written")
        logger.info("Minimum value : $minDate, Maximum Value : $maxDate")
        return IndexWriterStatistics(minDate, maxDate, validEntries.count(), errorCount+ skippedEntries)

    }
}


