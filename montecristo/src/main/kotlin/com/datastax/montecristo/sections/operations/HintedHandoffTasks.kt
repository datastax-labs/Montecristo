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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable


class HintedHandoffTasks : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)
        val hintsPerHourSet = mutableSetOf<Double>()
        // Live TP Stats figures
        val liveHintsMd = MarkdownTable("Node", "Active", "Pending" , "Completed", "Completed per Hr")

        cluster.nodes.forEach { node ->
            val rawHintLines = node.tpStats.data.filter { it.startsWith("HintsDispatcher") || it.startsWith("HintedHandoff") }
            if (rawHintLines.isNotEmpty()) {
                val chunks = rawHintLines.first().replace("\\s+".toRegex(), " ").split(" ")
                val mdRow = liveHintsMd.addRow().addField(node.hostname).addField(chunks[1]).addField(chunks[2]).addField(chunks[3])
                // only add to the table if there are active hints
                val uptime = cluster.getNode(node.hostname)?.info?.getUptimeInHours()
                if (uptime != null) {
                    val completedPerHour = Utils.round((chunks[3].toDoubleOrNull() ?: 0.0) / uptime)
                    hintsPerHourSet.add(completedPerHour)
                    mdRow.addField(completedPerHour)
                } else {
                    mdRow.addField("No Uptime Value.")
                }
            }
        }

        // Hinted Handoffs from the log files can have far more / less than JMX potentially
        val logHintedHandoffMessagesPerNode = logSearcher.search("+Finished +hinted +handoff +to +endpoint",
            limit = MESSAGE_SEARCH_LIMIT
        ).groupingBy { it.host }.eachCount()
        val hitLogMessageLimit = logHintedHandoffMessagesPerNode.size == MESSAGE_SEARCH_LIMIT // did we hit the search limit?

        val mapOfDurations: Map<String, Double> = cluster.getLogDurationsInHours(executionProfile.limits.numberOfLogDays)

        val logHintsMd = MarkdownTable("Node", "Hints Written", "Hints per Hour").orMessage("There were no hints dispatched within the log files.")
        for (n in logHintedHandoffMessagesPerNode.toList().sortedByDescending { it.second }) {
            val hintsPerHour = (n.second) / (mapOfDurations.getValue(n.first?:""))
            hintsPerHourSet.add(hintsPerHour)
            logHintsMd.addRow().addField(n.first?:"").addField(n.second).addField(Utils.round(hintsPerHour))
        }

        args["liveHints"] = liveHintsMd
        args["logHints"] = logHintsMd
        if (hitLogMessageLimit) {
            args["hitMessageLimit"] = hitLogMessageLimit.toString()
        }
        if (hitLogMessageLimit) {
            // this scenario is incredibly unlikely, but included just in case.
            recs.immediate(RecommendationType.OPERATIONS,"There are over $hitLogMessageLimit separate hint dispatch messages within the logs. We recommend further investigation into the high level of hint activity.")
        }
        val maxHintsPerHour = hintsPerHourSet.maxOf { it }
        if (maxHintsPerHour > HINTS_PER_HOUR_LIMIT) {
            recs.immediate(RecommendationType.OPERATIONS,"The highest average hints per hours for a node is ${Utils.round(maxHintsPerHour)}. A value this high indicates that the cluster is facing stability issues and should be investigated further.")
        }

        return compileAndExecute("operations/operations_hinted_handoff_tasks.md", args)
    }

    companion object {
        private const val MESSAGE_SEARCH_LIMIT = 1000000
        private const val HINTS_PER_HOUR_LIMIT = 25
    }
}