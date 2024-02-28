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
import com.datastax.montecristo.logs.logMessageParsers.GCPauseMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable
import java.time.format.DateTimeFormatter

class JvmGcStats : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // We get easily usable GCPause objects
        val gcPauses = logSearcher.search("GCInspector.java",
                limit = 1000000).map { GCPauseMessage.fromLogEntry(it) }

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")


        val mediumPauses = gcPauses.filter { it!!.timeInMS in 101..1000 }
                .groupingBy{ it!!.date.format(formatter) }
                .eachCount().toSortedMap()

        val longPauses = gcPauses.filter { it!!.timeInMS > 1000 }
                .groupingBy{ it!!.date.format(formatter) }
                .eachCount()

        if (longPauses.isNotEmpty()) {
            recs.immediate(
                RecommendationType.OPERATIONS,"We recommend investigating the cause of the long GC pauses (>1s) in the cluster. There are ${longPauses.values.sum()} such pauses reported in the logs.",
                    "We recommend investigating the cause of the long GC pauses (>1s) in the cluster.")
        }

        val countPerDay = gcPauses
                .groupingBy{ it!!.date.format(formatter) }
                .eachCount()

        val countPerDayTable = MarkdownTable("Day", "GC Pauses > 200ms", "GC Pauses > 1s")
        countPerDay.forEach {
            countPerDayTable.addRow()
                    .addField(it.key)
                    .addField(mediumPauses.getOrDefault(it.key, 0))
                    .addField(longPauses.getOrDefault(it.key, 0))
        }
        val distribution = gcPauses.groupingBy { getBucket(it!!.timeInMS) }
                .eachCount()

        val sortedDistribution = distribution.toSortedMap(compareBy { it.split(" ").first().trim().toDouble() })

        val sortedDistributionTable = MarkdownTable("Range", "Count")

        sortedDistribution.forEach { (t, u) ->
            sortedDistributionTable.addRow()
                    .addField(t)
                    .addField(u)
        }

        args["gcPauseTimesByDay"] = countPerDayTable.toString()
        args["sortedDistributionTable"] = sortedDistributionTable

        return compileAndExecute("operations/operations_jvm_gc_stats.md", args)
    }

    private fun getBucket(duration: Int): String {
        return when {
            duration  <= 200 -> "0.0 - 0.2 s"
            duration <= 300 -> "0.2 - 0.3 s"
            duration <= 400 -> "0.3 - 0.4 s"
            duration <= 500 -> "0.4 - 0.5 s"
            duration <= 1000 -> "0.5 - 1 s"
            duration <= 5000 -> "1 - 5 s"
            duration <= 10000 -> "5 - 10 s"
            duration <= 60000 -> "10 - 60 s"
            else -> "60 or higher"
        }
    }

}
