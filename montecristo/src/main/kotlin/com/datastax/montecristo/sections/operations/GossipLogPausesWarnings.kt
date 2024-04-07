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
import com.datastax.montecristo.logs.logMessageParsers.GossipLogPauseMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import java.time.temporal.ChronoUnit

class GossipLogPausesWarnings : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // search and parse the gossip failure detector messages
        val pauseWarnings = logSearcher.search(
            "FailureDetector.java",
            limit = executionProfile.limits.gossipPauseWarnings
        ).mapNotNull { GossipLogPauseMessage.fromLogEntry(it) }

        // first - did we hit the limit (and thus need to caveat any figures)
        val hitMessageLimit = pauseWarnings.size == executionProfile.limits.gossipPauseWarnings

        // Generate some statistical information, count, total, average duration, average duration, average per hour and %
        val countOfMessagesPerNode = pauseWarnings.groupingBy { it.host }.eachCount()
        val totalTimePausedPerNodeInMs = pauseWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.sumOf { it.pauseTimeInMs } }
        val averageTimePausedPerNodeInMs = pauseWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.map { it.pauseTimeInMs }.average() }

        val logDurations = cluster.metricServer.getLogDurations()
        val durationFromFirstToLast = logDurations.map  {
            val minDate = Utils.tryParseDate (it.value.first)
            val maxDate =  Utils.tryParseDate (it.value.second)
            val duration = minDate.until(maxDate, ChronoUnit.HOURS)
            Pair(it.key, duration.toDouble())
        }.toMap()

        val averagePausePerHour = durationFromFirstToLast.mapValues {
            (((totalTimePausedPerNodeInMs[it.key]?.toDouble() ?: 0.0) / 1000.0) / it.value)
        }
        val percentTimePause = averagePausePerHour.mapValues { (it.value / 3600.0) * 100 }

        // average of the exceeding time by node
        val countPerNodeTable = MarkdownTable("Host", "Number of Pauses", "Total Time Paused (s)", "Average Time Paused (s)", "Average Pause per Hour (s)", "% time paused")
        countOfMessagesPerNode.forEach {
            countPerNodeTable.addRow()
                    .addField(it.key)
                    .addField(it.value)
                    .addField(String.format("%.2f", (totalTimePausedPerNodeInMs[it.key] ?: 0).toDouble() / 1000.0))
                    .addField(String.format("%.2f", (averageTimePausedPerNodeInMs[it.key] ?: 0.0) / 1000.0))
                    .addField(String.format("%.2f", (averagePausePerHour[it.key])))
                    .addField(String.format("%.2f", (percentTimePause[it.key])))
        }

        val countOfWarningsMessage = if (hitMessageLimit) {
            "More than ${executionProfile.limits.gossipPauseWarnings} local pause warnings were discovered within the logs"
        } else {
            "A total of ${pauseWarnings.size} local pause warnings were discovered within the logs."
        }

        // TODO - what triggers the recommendation, % of time? max of a duration? avg duraation?
        if ((percentTimePause.maxByOrNull
            { it.value }?.value ?: 0.0) > executionProfile.limits.gossipPauseTimePercentageThreshold
        ) {
            recs.near(RecommendationType.OPERATIONS,"We recommend further investigation into the elevated level of local pause warnings.")
        }

        args["countOfWarnings"] = countOfWarningsMessage
        args["localPauseMessagesTable"] = countPerNodeTable.toString()
        return compileAndExecute("operations/operations_gossip_pause.md", args)
    }
}
