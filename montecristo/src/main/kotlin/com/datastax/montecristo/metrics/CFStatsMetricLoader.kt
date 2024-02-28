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

import com.datastax.montecristo.metrics.jmxMetrics.*
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.PreparedStatement

class CFStatsMetricLoader(val db: SqlLiteMetricServer, private val cfStats: File, val hostname: String) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun load() {

        val preparedStatementMap = mutableMapOf<String, PreparedStatement>()

        var ks = ""
        var completed: Long = 0

        val fileText = cfStats.readText()
        val keyspaceSections = fileText.split("----------------")

        for (section in keyspaceSections) {
            val sectionLines = section.split("\n")
            // first section will start with Total number of tables, we can ignore that one, we only want keyspace ones
            // there should be 6 lines as a minimum otherwise we can't process it (empty keyspace would be just the KS metrics, no tables)
            if (sectionLines.size < 6) {
                continue
            }
            ks = getValue(sectionLines, "Keyspace")

            // read the keyspace metrics off, before then processing the tables
            val metricList = mutableListOf<JMXMetric>()
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "ReadLatency")), "Count", getValue(sectionLines, "Read Count")))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "ReadLatency")), "Mean", getValue(sectionLines, "Read Latency")))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "ReadLatency")), "DurationUnit", "microseconds"))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "WriteLatency")), "Count", getValue(sectionLines, "Write Count")))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "WriteLatency")), "Mean", getValue(sectionLines, "Write Latency")))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "WriteLatency")), "DurationUnit", "microseconds"))
            metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "PendingFlushes")), "Value", getValue(sectionLines, "Pending Flushes")))

            // now handle Table Metrics - when we split it, the word Table is removed, so put it back!
            val tableSections = section.split("Table:").map { "Table:$it" }

            for (tableSection in tableSections) {
                val tableSectionLines = tableSection.split("\n")
                // check if its a table section, the keyspace header will be under 10 lines so this excludes it for us
                if (tableSectionLines.size < 10) {
                    continue
                }
                val tbl = getValue(tableSectionLines, "Table")

                metricList.add(TableHistogram(hostname, ks, tbl, "LiveSSTableCount", "Value", getValue(tableSectionLines, "SSTable count")))
                metricList.add(TableHistogram(hostname, ks, tbl, "LiveDiskSpaceUsed", "Count", getValue(tableSectionLines, "Space used (live)")))
                metricList.add(TableHistogram(hostname, ks, tbl, "TotalDiskSpaceUsed", "Count", getValue(tableSectionLines, "Space used (total)")))
                metricList.add(TableHistogram(hostname, ks, tbl, "SnapshotsSize", "Value", getValue(tableSectionLines, "Space used by snapshots (total)")))
                metricList.add(TableHistogram(hostname, ks, tbl, "CompressionRatio", "Value", getValue(tableSectionLines, "SSTable Compression Ratio")))
                // We won't know yet if this file is from C* 2.x or higher, so check the file for the 'normal' value anticipated, 3.x + otherwise try the 2.x
                if (tableSectionLines.any { it.trim().contains("Number of partitions") }) {
                    metricList.add(TableHistogram(hostname, ks,  tbl, "EstimatedPartitionCount", "Value", getValue(tableSectionLines, "Number of partitions")))
                } else {
                    metricList.add(TableHistogram(hostname, ks, tbl, "EstimatedPartitionCount", "Value", getValue(tableSectionLines, "Number of keys")))
                }
                metricList.add(TableHistogram(hostname, ks, tbl, "MemtableLiveDataSize", "Value", getValue(tableSectionLines, "Memtable data size")))
                metricList.add(TableHistogram(hostname, ks, tbl, "MemtableColumnsCount", "Value", getValue(tableSectionLines, "Memtable cell count")))
                metricList.add(TableHistogram(hostname, ks, tbl, "MemtableOffHeapSize", "Value", getValue(tableSectionLines, "Memtable off heap memory used")))
                metricList.add(TableHistogram(hostname, ks, tbl, "MemtableSwitchCount", "Value", getValue(tableSectionLines, "Memtable switch count")))

                metricList.add(TableHistogram(hostname, ks, tbl, "ReadLatency", "Count", getValue(tableSectionLines, "Local read count")))
                metricList.add(TableHistogram(hostname, ks, tbl, "ReadLatency", "Mean", getValue(tableSectionLines, "Local read latency")))

                metricList.add(TableHistogram(hostname, ks, tbl, "WriteLatency", "Count", getValue(tableSectionLines, "Local write count")))
                metricList.add(TableHistogram(hostname, ks, tbl, "WriteLatency", "Mean", getValue(tableSectionLines, "Local write latency")))

                metricList.add(TableHistogram(hostname, ks, tbl, "PendingFlushes", "Count", getValue(tableSectionLines, "Pending flushes")))
                metricList.add(TableHistogram(hostname, ks, tbl, "PercentRepaired", "Value", getValue(tableSectionLines, "Percent repaired")))

                metricList.add(TableHistogram(hostname, ks, tbl, "BloomFilterFalsePositives", "Value", getValue(tableSectionLines, "Bloom filter false positives")))
                metricList.add(TableHistogram(hostname, ks, tbl, "BloomFilterFalseRatio", "Value", getValue(tableSectionLines, "Bloom filter false ratio")))
                metricList.add(TableHistogram(hostname, ks, tbl, "BloomFilterDiskSpaceUsed", "Value", getValue(tableSectionLines, "Bloom filter space used")))
                metricList.add(TableHistogram(hostname, ks, tbl, "BloomFilterOffHeapMemoryUsed", "Value", getValue(tableSectionLines, "Bloom filter off heap memory used")))
                metricList.add(TableHistogram(hostname, ks, tbl, "IndexSummaryOffHeapMemoryUsed", "Value", getValue(tableSectionLines, "Index summary off heap memory used")))
                metricList.add(TableHistogram(hostname, ks, tbl, "CompressionMetadataOffHeapMemoryUsed", "Value", getValue(tableSectionLines, "Compression metadata off heap memory used")))

                metricList.add(TableHistogram(hostname, ks, tbl, "MinPartitionSize", "Value", getValue(tableSectionLines, "Compacted partition minimum bytes")))
                metricList.add(TableHistogram(hostname, ks, tbl, "MaxPartitionSize", "Value", getValue(tableSectionLines, "Compacted partition maximum bytes")))
                metricList.add(TableHistogram(hostname, ks, tbl, "MeanPartitionSize", "Value", getValue(tableSectionLines, "Compacted partition mean bytes")))
                metricList.add(KeyspaceHistogram(hostname, ks, mapOf(Pair("Name", "DroppedMutations")), "Count", getValue(tableSectionLines, "Dropped Mutations")))
            }

            metricList.forEach {
                val query = it.getPrepared()

                if (preparedStatementMap[query] == null) {
                    preparedStatementMap[query] = db.connection.prepareStatement(query)
                }
                val preparedStatement: PreparedStatement = preparedStatementMap[query]!!
                preparedStatement.clearParameters()
                it.bindAll(preparedStatement)

                try {
                    preparedStatement.executeUpdate()
                    completed++
                    if (completed % 5000 == 0L) {
                        logger.info("$completed metrics parsed for host ($hostname)")
                    }
                } catch (e: org.sqlite.SQLiteException) {
                    val resultCode = e.resultCode
                    if (resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY || resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT) {
                        logger.warn("Duplicate entry found, skipping. The query was '$it'")
                    }
                    if (it is Metric && it.mtype == "DroppedMessage") {
                        logger.warn("Query failed: $query, $e $it")
                    }
                    db.connection.clearWarnings()
                }
            }
        }

        logger.info("$completed metrics parsed for host ($hostname)")
    }

    fun getValue(data: List<String>, setting: String): String {
        val settingData = data.filter { it.trim().contains(setting) }
        return if (settingData.isNotEmpty()) {
            val splitData = settingData[0].replace("NaN", "0.0").split(":")
            if (splitData.size > 1) {
                // there could be a trailing unit measure sure as ms, so trim it, split based on the space and take the first part only
                val valueParts = splitData[1].trim().split(" ")
                // check if there are units
                when (valueParts.size) {
                    0 -> "" // no parts, whatever we had was a blank entry
                    1 -> valueParts[0] // no units ,just return the value
                    2 -> // values + units
                        if (valueParts[1].trim().toLowerCase() == "ms") {
                            // need to multiple it by 1k to get microseconds (because JMX expresses it as microseconds)
                            (valueParts[0].toDouble() * 1000.0).toString()
                        } else {
                            // not ms value - leave it as it is
                            valueParts[0]
                        }
                    else -> valueParts[0]  // if there are 3+ parts, we just return the first, this shouldn't happen but we need a else catch on the when
                }
            } else {
                "" // we failed to find a : on the setting line, store blank
            }
        } else {
            ""  // we couldn't find the setting, store a blank
        }
    }
}