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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable
import java.time.format.DateTimeFormatter
import java.util.*


class TombstoneWarnings : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)


        val results = cluster.databaseVersion.searchLogForTombstones(logSearcher, 1000000)

        val simpleDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val byDay = results.map { it.date.format(simpleDateFormatter)}.groupingBy { it }.eachCount()
        val byNode = results.map { it.host }.groupingBy { it }.eachCount()
        val byTableCounts = results.map { it.tableName }.groupingBy { it }.eachCount()
        val byTableLivePercentAvg = results.filter { it.liveRows > 0 }.groupBy { it.tableName }.mapValues { entry ->
            entry.value
                .map { message -> (message.tombstoneRows.toDouble() / (message.liveRows.toDouble() + message.tombstoneRows.toDouble())) }
                .average() * 100.0
        }

        val byDayTable = MarkdownTable("Day", "Log Messages")
        byDay.forEach {
            byDayTable.addRow().addField(it.key).addField(it.value)
        }

        val byNodeTable = MarkdownTable("Day", "Log Messages")
        byNode.forEach {
            byNodeTable.addRow().addField(it.key).addField(it.value)
        }

        val byDatabaseTable = MarkdownTable("Day", "Log Messages", "Average % Tombstone Rows")
        byTableCounts.forEach {
            byDatabaseTable.addRow().addField(it.key).addField(it.value).addField(
                if (byTableLivePercentAvg.containsKey(it.key)) {
                    String.format(Locale.ENGLISH, "%.2f", byTableLivePercentAvg[it.key])
                } else {
                    "-"
                }
            )
        }
        // Recommendation - trigger if more than 100 warnings in a single day.
        // This value is not normalized out based on the uptime per node, the complexity involved is too high vs the benefit to the trigger.
        if (byDay.filter { it.value >= executionProfile.limits.tombstoneWarningsPerDayThreshold }.size > 1) {
            recs.immediate(RecommendationType.DATAMODEL ,"We recommend reviewing the data model of ${byTableCounts.size} table(s), to remediate the number of tombstone warnings. DataStax Services can provide assistance in this activity.")
        }

        if (byDayTable.rows.size > 0) {
            args["byDay"] = byDayTable.toString()
            args["byNode"] = byNodeTable.toString()
            args["byTable"] = byDatabaseTable.toString()
        } else {
            args["noWarnings"] = "There were no tombstone log messages detected."
        }
        return compileAndExecute("operations/operations_tombstone_warnings.md", args)
    }
}
