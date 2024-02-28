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

package com.datastax.montecristo.metrics

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.datastax.montecristo.metrics.jmxMetrics.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.IOException
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime


/*
Class takes in a buffer which it can iterate over, yielding JMXMetric
objects.
These objects can generate their own SQL
 */
class MetricsReader(var host: String, var data: BufferedReader) {
    // yields a single metric at a time
    // to be transformed into a class later

    var logger: Logger = LoggerFactory.getLogger(this::class.java)!!

    fun iterJson() = sequence {

        // the json parser is not tolerant of a NaN value being in the file, even though they can appear.
        // during the re_org shell script, it has a hidden change of replacing NaN to 0.0, but when using the newer collector
        // this change is not made - and arguably the files should be left as collected and not altered.
        // The buffered reader provided to the file doesn't provide an opportunity to intercept - so we construct a string builder, buffered read all the lines
        // and map a string replacement on them before adding to the string builder. The parser takes a string builder so this works well.

        val baseConvertedLines = StringBuilder()
        data.readLines().forEach {
            if (it.contains("getting attribute Value")) {
                // this is the start of an exception for a value, we need to strip it out
                baseConvertedLines.append((it.substringBefore("getting attribute Value")).replace("NaN","0.0"))
            } else if (it.trim().startsWith("at") || it.trim().startsWith("Caused by")) {
                // skip these lines, they are exception lines.
            } else {
                baseConvertedLines.append(it.trim() + "\n")
            }
        }
        // there is a possibility that one of the errors occurred right in the middle of a NaN value, so once the error has been stripped out,
        // the NaN is then visible to be replaced. Previously we replaced this in the original loop, but the exception splitting the value
        // forces us to deal with it in a second pass iteration
        val convertedLines = StringBuilder()
        baseConvertedLines.toString().split("\n").forEach {
            convertedLines.append(it.replace("NaN","0.0") + "\n")
        }

        val json: JsonObject = Parser.default().parse(convertedLines) as JsonObject

        val beans = json.array<JsonObject>("beans")
        beans!!.forEach {
            if ((it.string("name") ?: "").contains(":")) {
                val metric = "${it.string("name")!!.split(":")[0]}{${it.string("name")!!.split(":")[1]}}[]"
                it.map.entries.filter { entry -> entry.key != "name" && entry.key != "modelerType" }
                        .forEach { entry ->
                            yield("${metric}${entry.key}: ${entry.value}")
                        }
            } else {
                // the value read has no : in it for the name, so we have no idea what this metric is, we need it to be ignored
                yield("")
                logger.info("Unable to parse metric json value " + it.string("name"))
            }
        }

    }

    fun iter() = sequence {
        while (true) {
            try {
                // buffered reader returns null at EOF, not an exception, has to be first!!
                val line = data.readLine() ?: break
                if (line.startsWith("org.")) {
                    yield (line)
                }
                continue
            } catch (e: IOException) {
                println("Reached end of file")
            }
        }
    }

    fun parseAndLoad(db: SqlLiteMetricServer, type: String) {
        val parser = MetricParser()
        var completed = 0
        val preparedStatementMap = mutableMapOf<String, PreparedStatement>()
        lateinit var query: String
        lateinit var parsedMetric: JMXMetric

        val iterator = if (type == "json") iterJson() else iter()
        for (item in iterator) {
            try {

                if (item == "" || item.startsWith("JMImplementation")     // not a metric
                        || item.startsWith("generation")        // payload of Net metrics, but current regexp can't do multiline
                ) {
                    continue
                }

                parsedMetric = parser.parseMetric(item, host)
                if (parsedMetric is Nio || parsedMetric is Request || parsedMetric is Net || parsedMetric is Auth || parsedMetric is Transport || parsedMetric is NoMatch) {
                    continue
                }

                query = parsedMetric.getPrepared()

                if (preparedStatementMap[query] == null) {
                    preparedStatementMap[query] = db.connection.prepareStatement(query)
                }

                val preparedStatement: PreparedStatement = preparedStatementMap[query]!!
                preparedStatement.clearParameters()
                parsedMetric.bindAll(preparedStatement)

                preparedStatement.executeUpdate()

                completed++
                if (completed % 50000 == 0) {
                    logger.info("$completed metrics parsed for host ($host)")
                }
            } catch (e: org.sqlite.SQLiteException) {
                val resultCode = e.resultCode
                if (
                        resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY
                        ||
                        resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT
                ) {
                    continue
                }
                if (parsedMetric is Metric && parsedMetric.mtype == "DroppedMessage") {
                    logger.warn("Query failed: $query, $e $parsedMetric")
                }
                db.connection.clearWarnings()
            } catch (e: IllegalStateException) {
                logger.warn("Could not parse $item, ${parsedMetric}")
            } catch (e: NotImplementedError) {
                logger.warn("Not implemented: $item")
            } catch (e: SQLException) {

                // BEWARE BUGS HERE
                if (parsedMetric is Metric && parsedMetric.mtype == "DroppedMessage") {
                    logger.warn("Query failed: $query, $e $parsedMetric")
                }
                db.connection.clearWarnings()
            }
        }

        logger.info("$completed metrics parsed for host ($host)")
    }
}

