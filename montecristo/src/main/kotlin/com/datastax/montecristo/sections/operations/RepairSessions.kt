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

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class RepairSessions : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        // replace with search
        val searchErrors = logSearcher.search("repair", LogLevel.ERROR, LIMIT)
        if (searchErrors.count() > 0) {
            args["numWarnings"] = searchErrors.count()
        }

        if (searchErrors.size >= LIMIT) {
            // hit the search limit
            args["hitLimit"] = LIMIT
        }

        val repairErrorCountByNode = searchErrors.groupingBy { it.host }.eachCount()
        val repairFailuresPerNodeTable = MarkdownTable("Host", "Number of Repair Failures")
        repairErrorCountByNode.toList().sortedByDescending {  (_, value) -> value }.toMap().forEach {
            repairFailuresPerNodeTable.addRow()
                .addField(it.key?: "Unknown Node")
                .addField(it.value)
        }
        args["warningsByNode"] = repairFailuresPerNodeTable.toString()

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val repairErrorCountByDay = searchErrors.groupingBy { it.getDate().truncatedTo(ChronoUnit.DAYS) }.eachCount()
        val repairFailuresPerDateTable = MarkdownTable("Date", "Number of Repair Failures")
        repairErrorCountByDay.toList().sortedByDescending { (value, count) -> value }.take(NUMBER_OF_DATES_TO_REPORT).toMap().forEach {
            repairFailuresPerDateTable.addRow()
                .addField(it.key.format(formatter) ?: "Unknown Date" )
                .addField(it.value)
        }

        if (searchErrors.size > 250) {
            recs.immediate(RecommendationType.OPERATIONS,"Repairs on the cluster are failing in sufficient numbers to be a cause of concern. We recommend investigating the root cause of the failures and impact to data consistency at rest and when served for reads.")
        }
        args["numberOfDatesToReport"] = NUMBER_OF_DATES_TO_REPORT
        args["warningsByDate"] = repairFailuresPerDateTable.toString()

        // are repairs running?
        val logDurations = cluster.metricServer.getLogDurations()
        // calculate the latest log date, minus 10 days
        val maxLogDate = logDurations.map { Utils.tryParseDate (it.value.second ) }.maxByOrNull { it } ?: LocalDateTime.now()

        val repairEntries = logSearcher.search("RepairSession.java", LogLevel.INFO, LIMIT)
        val isRepairRunning = repairEntries.any { it.getDate().isAfter(maxLogDate.minusDays(10)) }
        if (isRepairRunning) {
            args["repairsFound"] = "Repairs are being run on the cluster."
        } else {
            if (cluster.nodes.count() > 1) {
                args["repairsFound"] = "No evidence in the logs was found indicating that repairs are running recently."
                recs.near(
                    RecommendationType.OPERATIONS,
                    "We recommend that repairs are run on the cluster to ensure data consistency across the nodes."
                )
            } else {
                args["repairsFound"] = "The cluster is a single node and running repair is not needed."
            }
        }

        // incremental repairs
        if (cluster.isDse) {
            args["isDse"] = true
        } else {
            args["isCassandra"] = true
        }

        if (!cluster.databaseVersion.supportsIncrementalRepair()) {
            if (cluster.nodes.any { it.ssTableStats.data.any { ssTable -> ssTable.incrementallyRepaired } }) {
                recs.near(RecommendationType.OPERATIONS, "We recommend that you do not use incremental repairs, and migrate to full repairs on all keyspaces.")
            } else{
                args["incremental"] = "No use of incremental repairs was detected and we recommend that it is not used."
            }
        } else {
            args["incremental"] = "No use of incremental repairs was detected."
        }
        return compileAndExecute("operations/operations_logs_failed_repair.md", args)
    }

    companion object {
        private const val LIMIT = 100000
        private const val NUMBER_OF_DATES_TO_REPORT = 14
    }

}
