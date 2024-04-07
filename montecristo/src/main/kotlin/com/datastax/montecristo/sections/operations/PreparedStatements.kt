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
import com.datastax.montecristo.logs.logMessageParsers.PreparedStatementDiscardedMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable
import java.time.format.DateTimeFormatter

class PreparedStatements : DocumentSection {

    private val remediationMessage = "We recommend checking the prepared statement cache and identify incorrectly prepared statements from the application."

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        // search through the warnings, up to 1 million
        val preparedStatementDiscardedMessageWarnings =
            logSearcher.search("+prepared +statements +discarded", LogLevel.WARN, executionProfile.limits.preparedStatementWarnings)
                .mapNotNull { PreparedStatementDiscardedMessage.fromLogEntry(it) }

        // The log files can have far more warnings potentially
        val numWarnings = Utils.humanReadableCount(preparedStatementDiscardedMessageWarnings.size.toLong())

        val hitMessageLimit = preparedStatementDiscardedMessageWarnings.size >= executionProfile.limits.preparedStatementWarnings
        args["hitMessageLimit"] = hitMessageLimit
        if (hitMessageLimit) {
            args["numWarnings"] = Utils.humanReadableCount(executionProfile.limits.preparedStatementWarnings.toLong())
        } else {
            args["numWarnings"] = numWarnings
        }

        // need the warnings by node to compare against the durations, if any node exceeds the threshold, then we warn.
        val mapOfDurations: Map<String, Double> = cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays)
        val countOfMessagesPerNode = preparedStatementDiscardedMessageWarnings.groupingBy { it.host }.eachCount()
        val preparedStatementsDiscardedRecommendationExceeded = mapOfDurations.any { node ->
            (countOfMessagesPerNode.getOrDefault(
                node.key,
                0
            ) / mapOfDurations.getValue(node.key) > executionProfile.limits.preparedStatementMessagesPerHourThreshold)
        }

        // Warnings aggregated by Date - for the last 7 days
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val warningsByDate = preparedStatementDiscardedMessageWarnings.groupingBy { node -> node.date.format(formatter) }.eachCount()
                .toSortedMap(Comparator.reverseOrder()).toList().take(14)
        val warningsByDateMd = MarkdownTable(
            "Date",
            "Warnings"
        ).orMessage("")
        if (warningsByDate.isNotEmpty()) {
            for (aggDay in warningsByDate) {
                warningsByDateMd.addRow()
                    .addField(aggDay.first)
                    .addField(aggDay.second)
            }
        }
        args["warningsByDateTable"] = warningsByDateMd.toString()

        if (hitMessageLimit) {
            // this scenario is incredibly unlikely, but included just in case.
            recs.immediate(
                RecommendationType.OPERATIONS,
                "There are over $numWarnings prepared statement discard warnings within the logs. $remediationMessage"
            )
        } else if (preparedStatementsDiscardedRecommendationExceeded) {
            // trigger a recommendation if the number of warnings exceeds the trigger limit
            recs.immediate(
                RecommendationType.OPERATIONS,
                "There are over ${executionProfile.limits.preparedStatementMessagesPerHourThreshold} prepared statement discard warnings on average every hour. $remediationMessage"
            )
        }
        return compileAndExecute("operations/operations_prepared_statements_discarded.md", args)
    }
}

