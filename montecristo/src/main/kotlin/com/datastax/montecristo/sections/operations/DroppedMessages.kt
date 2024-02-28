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
import com.datastax.montecristo.logs.logMessageParsers.DroppedOperationMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable

/**
 * Reports on the various dropped messages.  Currently we only have mutations and reads but it's easy to add more.
 */
class DroppedMessages : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // dropped mutations - from metrics DB / JMX
        val totalDroppedMutations = cluster.metricServer.getDroppedCounts("MUTATION").values.sum()

        val jmxDropMd = MarkdownTable("Node", "Mutations", "Reads").orMessage("There were no dropped mutations within the JMX metrics.")
        for (n in cluster.metricServer.getDroppedCounts("MUTATION").toList().sortedByDescending { it.second }.toMap()) {
            val droppedReads = cluster.metricServer.getDroppedCounts("READ").getOrDefault(n.key, 0)
            // filter out nodes which do not have drops.
            if (droppedReads >0 && n.value > 0) {
                jmxDropMd.addRow().addField(n.key).addField(n.value)
                    // get dropped reads
                    .addField(cluster.metricServer.getDroppedCounts("READ").getOrDefault(n.key, 0))
            }
        }

        // The log files can have far more warnings potentially
        val logDroppedMessageWarnings = logSearcher.search(
            "+MUTATION +messages +were +dropped +in +the +last",
            limit = LIMIT
        ).mapNotNull { DroppedOperationMessage.fromLogEntry(it) }
        val hitMessageLimit = logDroppedMessageWarnings.size == LIMIT // did we hit the search limit?
        // map of node to mutation values, (count of messages, sum of internal drops and sum of cross node drops)
        val sortedResults = processDroppedMutationLogMessages(logDroppedMessageWarnings)
        // map of node to log duration
        val mapOfDurations: Map<String, Double> = cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays)

        var dropRecommendationExceeded = false
        val logDropMd = MarkdownTable("Node", "Log Duration (Days)", "Total Internal Mutation Drops", "Total Cross-Node Mutations Dropped", "Number of Distinct Hours with Mutations Dropped", "Drops per Hour").orMessage("There were no dropped mutations within the logs.")
        for (node in sortedResults) {
            val droppedMutationsPerHour = (node.value.sumOfInternalDrops + node.value.sumOfCrossNodeDrops) / (mapOfDurations.getValue(node.key))
            if (droppedMutationsPerHour > DROP_PER_HOUR_LIMIT) {
                dropRecommendationExceeded = true
            }
            logDropMd.addRow()
                    .addField(node.key)
                    .addField(String.format("%.2f", mapOfDurations.getValue(node.key) / 24.0))
                    // get dropped reads
                    .addField(node.value.sumOfInternalDrops)
                    .addField(node.value.sumOfCrossNodeDrops)
                    .addField(node.value.numberOfHoursWithDrops)
                    .addField(String.format("%.2f", droppedMutationsPerHour))
        }

        args["jmxDroppedMutations"] = jmxDropMd
        args["totalDroppedMutations"] = totalDroppedMutations.toString()
        args["logDroppedMutations"] = logDropMd
        if (hitMessageLimit) {
            args["hitMessageLimit"] = hitMessageLimit.toString()
        }
        if (hitMessageLimit) {
            // this scenario is incredibly unlikely, but included just in case.
            recs.immediate(RecommendationType.OPERATIONS,"There are over $hitMessageLimit separate dropped mutation warnings within the logs. We recommend further investigation into the extremely high level of dropped mutations.")
        } else if ( dropRecommendationExceeded) {
            // trigger a recommendation if the number of dropped messages per hour exceeds the trigger limit
            recs.immediate(RecommendationType.OPERATIONS,"There are over $DROP_PER_HOUR_LIMIT dropped mutations occurring on average every hour. We recommend further investigation into this elevated level of dropped mutations.")
        }
        return compileAndExecute("operations/operations_dropped_messages.md", args)
    }

    private fun processDroppedMutationLogMessages(logDroppedMessageWarnings: List<DroppedOperationMessage>): Map<String, DroppedMutationMessageCounts> {
        // Count of messages per node
        val countOfMessagesPerNode = logDroppedMessageWarnings.groupingBy { it.host }.eachCount()
        val sumOfInternalDrops = logDroppedMessageWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.sumOf { it.internalDrops } }
        val sumOfCrossNodeDrops = logDroppedMessageWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.sumOf { it.crossNodeDrops } }
        // grouping up by the hour
        val messagesGroupedByNodeHour = logDroppedMessageWarnings.groupingBy { it.host + "_" + it.date.year + "_" + it.date.dayOfYear + "_" + it.date.hour }.eachCount()
        val messagesNumberOfHoursPerNode = messagesGroupedByNodeHour.toList().groupingBy  { it.first.split("_")[0] }.eachCount()

        //  4 maps, of which, entries in all 3 are guaranteed - need putting together so that we can sort them.
        val results: MutableMap<String, DroppedMutationMessageCounts> = mutableMapOf()
        countOfMessagesPerNode.forEach {
            results[it.key] =
                DroppedMutationMessageCounts(it.value, sumOfInternalDrops.getOrDefault(it.key, 0), sumOfCrossNodeDrops.getOrDefault(it.key, 0), messagesNumberOfHoursPerNode.getOrDefault(it.key, 0))
        }
        return results.toList().sortedBy { it.second.sumOfInternalDrops + it.second.sumOfCrossNodeDrops }.reversed().toMap()
    }

    // data class just to hold the results of the maps together
    private data class DroppedMutationMessageCounts( val countOfMessages : Int, val sumOfInternalDrops : Long, val sumOfCrossNodeDrops : Long, val numberOfHoursWithDrops : Int)
    companion object {
        private const val LIMIT = 1000000
        private const val DROP_PER_HOUR_LIMIT = 25
    }
}
