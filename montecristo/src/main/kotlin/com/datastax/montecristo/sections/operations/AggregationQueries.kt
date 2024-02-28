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
import com.datastax.montecristo.logs.logMessageParsers.AggregationQueryMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.HumanCount
import com.datastax.montecristo.utils.MarkdownTable
import java.time.format.DateTimeFormatter

class AggregationQueries : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        // search through the warnings, up to 1 million
        val aggregationMessageWarnings = logSearcher.search("+Aggregation +query", LogLevel.WARN, executionProfile.limits.aggregationWarnings).mapNotNull { AggregationQueryMessage.fromLogEntry(it) }

        // The log files can have far more warnings potentially
        val numWarnings = Utils.humanReadableCount(aggregationMessageWarnings.size.toLong())
        val hitMessageLimit = aggregationMessageWarnings.size >= executionProfile.limits.aggregationWarnings
        args["hitMessageLimit"] = hitMessageLimit
        args["limit"] = Utils.humanReadableCount(executionProfile.limits.aggregationWarnings.toLong())
        args["numWarnings"] = numWarnings

        // need the warnings by node to compare against the durations, if any node exceeds the threshold, then we warn.
        val mapOfDurations: Map<String, Double> = cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays)
        val countOfMessagesPerNode = aggregationMessageWarnings.groupingBy { it.host }.eachCount()
        val aggregationRecommendationExceeded = mapOfDurations.any { node ->
            (countOfMessagesPerNode.getOrDefault(node.key,0) / mapOfDurations.getValue(node.key)) > AGGREGATION_PER_HOUR_LIMIT
        }

        // Warnings aggregated by Date - for the last 7 days
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val aggregationsByDay = aggregationMessageWarnings.groupingBy { node -> node.date.format(formatter)  }.eachCount().toSortedMap(Comparator.reverseOrder()).toList().take(14)
        val aggregationByDateMd = MarkdownTable("Date", "Warnings").orMessage("There were no aggregation query warnings within the logs.")
        for (aggDay in aggregationsByDay) {
                aggregationByDateMd.addRow()
                .addField(aggDay.first)
                .addField(aggDay.second)
        }
        args["warningsByDate"] = aggregationByDateMd.toString()


        // Warnings aggregated by Table - across all time in the logs.
        val aggregationByTableMd = MarkdownTable("Table", "Total Number of Aggregation Queries").orMessage("")
        val aggregationsByTable = aggregationMessageWarnings.groupingBy { entry -> entry.keyspaceTable }.eachCount()
        for (table in aggregationsByTable) {
            aggregationByTableMd.addRow()
                .addField(table.key)
                .addField(table.value)
        }
        args["warningsByTable"] = aggregationByTableMd.toString()

        if (hitMessageLimit) {
            // this scenario is incredibly unlikely, but included just in case.
            recs.immediate(RecommendationType.OPERATIONS,"There are over $numWarnings aggregation query warnings within the logs. $remediationMessage")
        } else if ( aggregationRecommendationExceeded) {
            // trigger a recommendation if the number of dropped messages per hour exceeds the trigger limit
            recs.immediate(RecommendationType.OPERATIONS,"There are over $AGGREGATION_PER_HOUR_LIMIT aggregation queries occurring on average every hour. $remediationMessage")
        }

        return compileAndExecute("operations/operations_aggregation_queries.md", args)
    }

    private val remediationMessage = "We recommend investigating the purpose and source of these queries, and investigating alternative approaches to the business need they are used to address."

    companion object {
        private const val AGGREGATION_PER_HOUR_LIMIT = 5
    }
}
