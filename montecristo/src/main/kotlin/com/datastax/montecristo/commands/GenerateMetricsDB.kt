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

package com.datastax.montecristo.commands

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import com.datastax.montecristo.helpers.FileHelpers
import com.datastax.montecristo.logs.LogFileFinder
import com.datastax.montecristo.logs.LogIndexer
import com.datastax.montecristo.logs.LogRegex
import com.datastax.montecristo.metrics.CFStatsMetricLoader
import com.datastax.montecristo.metrics.MetricsReader
import com.datastax.montecristo.metrics.Server
import com.datastax.montecristo.metrics.SqlLiteMetricServer
import com.datastax.montecristo.model.logs.LogbackAppender
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.input.ReversedLinesFileReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory
import org.sqlite.SQLiteException
import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.nio.charset.Charset
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger

@Parameters(commandDescription = "Generate the metrics database and the lucene search index.")
class GenerateDB {

    // we can avoid the other stuff with this
    @Parameter(description = "Directory to analyze")
    var directory: String = ""


    @Parameter(names = ["--overwrite"])
    var overwrite: Boolean = false

    @Parameter(names = ["--skiplogs"])
    var skiplogs: Boolean = false

    fun execute() {
        val p = Paths.get(directory).toAbsolutePath().toString()
        GenerateMetricsDB(p, overwrite, skiplogs).execute()
    }
}

class GenerateMetricsDB(
    val directory: String,
    private val overwrite: Boolean,
    val skiplogs: Boolean,
    val skipMetrics : Boolean = false,
    val parallelProcessJmx: Boolean = false, // This setting is for testing purposes where we want parallel to be false.
    val parallelProcessLogs: Boolean = true
) {
    private val numberOfNodesMetricsProcessed = AtomicInteger(0)
    private val numberOfNodesLogsProcessed = AtomicInteger(0)

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun execute() {
        println("Using $directory")
        val dbLocation = File(directory, "metrics.db")
        val extractedLocation = File(directory, "extracted")
        val logSearchIndexLocation = File(directory, "logSearchIndex")

        if (!skipMetrics) {
            println("Creating DB at $dbLocation")
            if (dbLocation.exists() and !overwrite) {
                throw FileAlreadyExistsException(dbLocation)
            } else if (dbLocation.exists()) {
                // --overwrite flag was provided
                dbLocation.delete()
                logSearchIndexLocation.deleteRecursively()
            }
        }

        // iterate over each directory in the input directory
        val directories = extractedLocation.listFiles(File::isDirectory).toList()
        if (duplicateNodesExist(directories)) {
            throw java.lang.RuntimeException("Duplicate node folders detected")
        }

        val indexWriter = LogIndexer.getWriter(FSDirectory.open(logSearchIndexLocation.toPath()))

        // for each directory, iterate over the metrics and insert metrics into the DB
        val loadDbSchema = true
        // create the object to make sure the schema is in place before loading.
        val dao = SqlLiteMetricServer(dbLocation.absolutePath, loadDbSchema)

        if (!skipMetrics) {
            // performance optimization, like 100000x faster

            dao.connection.autoCommit = false

            // This setting exists for testing purposes, where we don't want the files done in parallel (makes debugging much harder)
            if (parallelProcessJmx) {
                directories.parallelStream().forEach {
                    // for a parallel load, we need 1 DAO per thread, because of prepared statements and thread safety
                    val parallelLoadingDao = SqlLiteMetricServer(dbLocation.absolutePath, false)
                    // performance optimization, like 100000x faster
                    parallelLoadingDao.connection.autoCommit = false
                    processDirectoryJmx(it, parallelLoadingDao)
                    parallelLoadingDao.connection.commit()
                    // the object is going to disappear, close the connection before it does
                    parallelLoadingDao.connection.close()
                }
            } else {
                directories.forEach {
                    processDirectoryJmx(it, dao)
                    // one big commit of everything, seems to be the fastest thing to do
                    dao.connection.commit()
                }
            }
        }
        dao.connection.autoCommit = true
        if (!skiplogs) {
            if (parallelProcessLogs) {
                // grouping the files into sub-lists of 5 elements each to avoid going OOM
                val groupedDirectories = directories.chunked(5)
                groupedDirectories.forEach { gd ->
                    // run each group of 5 in parallel
                    gd.parallelStream().forEach {
                        val parallelLoadingDao = SqlLiteMetricServer(dbLocation.absolutePath, false)
                        processDirectoryLogs(it, parallelLoadingDao, indexWriter)
                        parallelLoadingDao.connection.close()
                    }
                }
            } else {
                directories.forEach {
                    processDirectoryLogs(it, dao, indexWriter)
                }
            }
            indexWriter.close()
        }
    }

    private fun processDirectoryJmx(
        dir: File,
        dao: SqlLiteMetricServer
    ) {
        // we're going to use the hostname in the database, we pull it out of os/hostname for each directory
        val hostname = FileHelpers.getHostname(dir)

        println(hostname)
        logger.info("Processing metrics for host '$hostname'")

        // We need to figure out the metrics source, there is a priority order
        // we need a buffered reader for the metrics reader
        var metricsFileName: String
        val metricsFileType: String
        val jmxFile = File(dir, "metrics.jmx")
        val jsonFile = File(dir, "jmx_dump.json")
        val cfStatsFile = FileHelpers.getFile(File(dir, "nodetool"), listOf("cfstats"))

        if (jmxFile.exists() && jmxFile.length() > 0) {
            metricsFileName = "metrics.jmx"
            metricsFileType = "plain"
        } else if (jsonFile.exists() && jsonFile.length() > 0) {
            metricsFileName = "jmx_dump.json"
            metricsFileType = "json"
        } else {
            if (cfStatsFile.exists() && cfStatsFile.length() > 0) {
                metricsFileName = cfStatsFile.name
                metricsFileType = "cfstats"
            } else {
                // we have no metrics file to work with - let it use the default and fail
                metricsFileName = "metrics.jmx"
                metricsFileType = "plain"
            }
        }

        try {
            val hasJmx = if (metricsFileType == "plain" || metricsFileType == "json") {
                try {
                    val reader = BufferedReader(FileReader(File(dir, metricsFileName)))
                    MetricsReader(hostname, reader).parseAndLoad(dao, metricsFileType)
                    true
                } catch (e: RuntimeException) {
                    logger.warn("Failed to parse the metrics file : $metricsFileName for host ${hostname}. Falling back to CFStats")
                    // JMX failed, we should attempt to use CFStats instead - if it does not exist, we will hit a failure again
                    metricsFileName = cfStatsFile.name
                    CFStatsMetricLoader(dao, cfStatsFile, hostname).load()
                    false
                }
            } else {
                // CFStats to be loaded instead of metrics
                CFStatsMetricLoader(dao, cfStatsFile, hostname).load()
                false
            }
            Server(dao, dir, hostname, hasJmx).load()
        } catch (e: FileNotFoundException) {
            logger.warn("No metrics file : $metricsFileName for host ${hostname}.", e)
        } catch (e: RuntimeException) {
            logger.warn("Failed to parse $metricsFileName for host ${hostname}.", e)
        }

        logger.info("Processed Metrics for : $hostname - Nodes Metrics Processed = ${numberOfNodesMetricsProcessed.incrementAndGet()}")
    }

    private fun processDirectoryLogs(
        dir: File,
        dao: SqlLiteMetricServer,
        indexWriter: IndexWriter
    ) {
        // we're going to use the hostname in the database, we pull it out of os/hostname for each directory
        val hostname = FileHelpers.getHostname(dir)
        // map for storing min and max log dates
        val logDates = mutableMapOf<String, Pair<LocalDateTime, LocalDateTime>>()

        // reset the log dates for this node.
        var minLogDate: LocalDateTime = LocalDateTime.MAX
        var maxLogDate: LocalDateTime = LocalDateTime.MIN

        println(hostname)
        logger.info("Processing logs for host '$hostname'")

        if (!skiplogs) {

            logger.info("Loading logs")
            val logFilesAndAppender: Pair<List<File>, LogbackAppender> = LogFileFinder.getLogFiles(dir)
            val logFiles = logFilesAndAppender.first
            val appender = logFilesAndAppender.second
            val regex = LogRegex.convert( appender.encoderPattern)

            // TODO - Raise exception, if no log files to process.
            for (logFile in logFiles) {
                try {
                    // The original code hashed the entire file, but if the file is very large, then the hashing takes an incredibly long time and must
                    // load the entire file through to get to the hash value. Changed to hash based on the first 100 and last 100 lines.
                    val hashValue = calculateFileHash(logFile)
                    dao.insertDBValues(
                        hostname,
                        "",
                        "logFileHash",
                        hashValue,
                        ""
                    )

                    logger.info("Loading $logFile")

                    val logEntries = LogIndexer.getLogEntries(logFile.inputStream(), regex)
                    val indexWriterStats = LogIndexer.writeToIndex(indexWriter, logEntries, hostname)
                    if (indexWriterStats.minDate < minLogDate) {
                        minLogDate = indexWriterStats.minDate
                    }
                    if (indexWriterStats.maxDate > maxLogDate) {
                        maxLogDate = indexWriterStats.maxDate
                    }
                    dao.insertDBValues(hostname, "log_stats",logFile.absolutePath, "valid_entries", indexWriterStats.validEntries.toString() )
                    dao.insertDBValues(hostname, "log_stats",logFile.absolutePath, "invalid_entries", indexWriterStats.invalidEntries.toString() )

                } catch (e: SQLiteException) {
                    logger.warn(
                        "Skipping duplicate log file $logFile for $hostname",
                        e
                    ) // we carry on regardless however
                }
            }

            // we have a min / max date for the logs for this node
            if (minLogDate != LocalDateTime.MAX) {
                // we found some entries, add this node the logging map
                logDates[hostname] = Pair(minLogDate, maxLogDate)
            }
            // we now need to store down these entries so that we can access them later, we can't calc this on the fly
            // and the metrics DB generation can be done independent to the report
            val internalFormat = "yyyy-MM-dd'T'HH:mm:ss"
            val formatter = DateTimeFormatter.ofPattern(internalFormat)
            try {
                dao.insertDBValues(hostname, "", "", "minLogDate", minLogDate.format(formatter))
            } catch (e: SQLiteException) {
                logger.warn(
                    "Unable to save the log minimum date for the $hostname",
                    e
                ) // we carry on regardless however
            }
            try {
                dao.insertDBValues(hostname, "", "", "maxLogDate", maxLogDate.format(formatter))
            } catch (e: SQLiteException) {
                logger.warn(
                    "Unable to save the log maximum date for the $hostname",
                    e
                ) // we carry on regardless however
            }
        } else {
            logger.warn("skipping logs")
        }
        logger.info("Processed Logs for : $hostname - Nodes Logs Processed = ${numberOfNodesLogsProcessed.incrementAndGet()}")
    }

    internal fun calculateFileHash(logFile: File): String {
        // read the first 100 lines in a lazy fashion, this prevents the whole file coming into memory, which can cause issues.
        val reader = BufferedReader(FileReader(logFile))
        val firstLines = reader.useLines { lines: Sequence<String> -> lines.take(100).toList() }.joinToString("\n")
        reader.close()
        // use reverse reader to read the last 100 lines (if it reaches the start, the reader returns null, which exits the loop)
        val reverseReader = ReversedLinesFileReader(logFile, Charset.defaultCharset())
        var counter = 0
        var line: String? = ""
        val lastLines = mutableListOf<String>()
        while (counter <= 100 && line != null) {
            line = reverseReader.readLine()
            if (line != null) {
                lastLines.add(line)
            }
            counter++
        }
        reverseReader.close()
        // Using DigestUtils because MessageDigest is not thread-safe and this can be called in parallel.
        val rawHashBytes = DigestUtils.sha256(firstLines.plus(lastLines.joinToString("\n")))
        return rawHashBytes.fold("") { str, it -> str + "%02x".format(it) }
    }

    private fun duplicateNodesExist(directories: List<File>): Boolean {
        val pattern = Regex("(.*)(_artifacts)")
        val listOfNodeName = directories.map {
            pattern.find(it.name)?.groups?.get(1)?.value ?: it.name
        }.toList()
        // if the set size is not equal to the list size, there are duplicates
        return listOfNodeName.size != listOfNodeName.toSet().size
    }
}