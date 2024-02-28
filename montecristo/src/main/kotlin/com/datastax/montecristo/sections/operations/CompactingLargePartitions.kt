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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.helpers.humanBytes
import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlin.math.pow

class CompactingLargePartitions : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val logEntries = cluster.databaseVersion.searchLogForLargePartitionWarnings(logSearcher, MESSAGE_SEARCH_LIMIT)

        if (logEntries.isNotEmpty()) {
            val largePartitionsByTableMd = MarkdownTable("Table", "Number of Warnings")
            val partitionsByTable = countByKeyspaceTable(logEntries.map { entry -> entry.message!! })
            partitionsByTable.sortedByDescending { it.second }
                .forEach { largePartitionsByTableMd.addRow().addField(it.first).addField(it.second) }
            args["largePartitionsWarningsByTable"] = largePartitionsByTableMd.toString()

            val largePartitionsByDateMd = MarkdownTable("Date", "Number of Warnings")
            val partitionsByDate = countByDate(logEntries)
            partitionsByDate.forEach { largePartitionsByDateMd.addRow().addField(it.first).addField(it.second) }
            args["largePartitionsWarningsByDate"] = largePartitionsByDateMd.toString()

            val largePartitionsPerTableMd = MarkdownTable("Table", "Size")
            val partitionsBySize = getSortedPartitionSizes(cluster, logEntries.map { entry -> entry.message!! })
            partitionsBySize.forEach { largePartitionsPerTableMd.addRow().addField(it.first).addField(it.second.humanBytes()) }
            args["largestPartitionsPerTable"] = largePartitionsPerTableMd.toString()

            if (partitionsByTable.any { it.first == "system_distributed.repair_history" } ) {
                recs.near(RecommendationType.OPERATIONS, "We recommend truncating the contents of the system_distributed.repair_history table when the repair process is not running, to eliminate the large partitions within the table.")
            }
        }

        // 2^20 = 2^10^10  = 1MB
        val partitionSizeThreshold = 100
        val matchingTables = cluster.schema.tables.filter {
            (it.maxPartitionSize.value.max() ?: 0.0) > 2.0.pow(20.0) * partitionSizeThreshold
        }

        val largePartitionsTable = MarkdownTable("Table", "Largest Partition").orMessage("No partitions over 100 MB have been tracked.")
        matchingTables
            .sortedByDescending { it.maxPartitionSize.value.max() }
            .forEach {
                largePartitionsTable.addRow()
                    .addField(it.name)
                    .addField(it.maxPartitionSize.value.max()!!.humanBytes())
            }
        val largePartitionCount = matchingTables.count()

        args["largePartitionsTableFromMetrics"] = largePartitionsTable

        if (largePartitionCount > 0) {
            recs.immediate(
                RecommendationType.OPERATIONS,
                "We recommend reviewing the data model to determine hot spots and wide partitions. Tables with partitions over ${
                    (partitionSizeThreshold * 1000 * 1000).toLong().humanBytes()
                } require special attention as they are a direct threat to cluster stability. " +
                        "There are $largePartitionCount such tables in the cluster.\n"
            )
        }

        return compileAndExecute("operations/operations_compacting_large_partitions.md", args)
    }

    companion object {
        fun getSortedPartitionSizes(cluster: Cluster, largeCompactionLogs: List<String>): List<Pair<String, Long>> {

            val parsedLogs = cluster.databaseVersion.parseLargePartitionSizeMessage(largeCompactionLogs)
            // at this point we have a list Pair("ks/t:partition", sizeInBytes), so we group by partition
            return parsedLogs.groupBy { it.first }
                .entries
                // keep the largest value per partition
                .map { entry -> Pair(entry.key, entry.value.maxByOrNull { it.second }!!.second) }
                // sort by size desc
                .sortedByDescending { it.second }
                .take(200)
                // and return as a list of pairs
                .map { (keyspaceTablePartition, size) -> Pair(keyspaceTablePartition, size) }
        }

        /**
         * Compacting large partition keyspace/table:<primary_key>
         */
        private fun countByKeyspaceTable(largeCompactionLogs: List<String>): List<Pair<String, Int>> {
            val pattern = Regex("(partition|row) ([^/]+)/([^:]+):")
            return largeCompactionLogs
                // parse the keyspace and table
                .map { line -> pattern.find(line)?.groupValues }
                // try to access the values, default to unknown
                .map { group -> "${group?.get(2) ?: "UNKNOWN_KS"}.${group?.get(3) ?: "UNKNOWN_T"}" }
                // at this point we have a list of "ks.t" strings, so we simply group & count them
                .groupingBy { it }
                .eachCount()
                .entries
                // sort by number of occurrences
                .sortedByDescending { (keyspaceTable, count) -> count }
                // and return as a list of pairs
                .map { (keyspaceTable, count) -> Pair(keyspaceTable, count) }
        }

        internal fun countByDate(largeCompactionLogs: List<LogEntry>): List<Pair<String, Int>> {
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            return largeCompactionLogs
                .map { entry -> entry.getDate() }
                .groupingBy { it.truncatedTo(ChronoUnit.DAYS ) }
                .eachCount()
                .toList()
                .sortedByDescending { it.first }
                .map { entry -> Pair( entry.first.format(formatter), entry.second) }
        }

        private const val MESSAGE_SEARCH_LIMIT = 1000000
    }
}

