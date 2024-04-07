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
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable

class DroppedHints : DocumentSection {
	
	override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

		val droppedHintsResult = logSearcher.search("dropped AND hints", LogLevel.WARN, limit = executionProfile.limits.droppedHints)

		val droppedMessagedRegex = """([^\s]*) has ([\d]*) dropped""".toRegex()

		val matches = droppedHintsResult.map{
			val matches = droppedMessagedRegex.find(it.message!!)
			matches
		}.map { Pair(it!!.groups[1]!!.value, it.groups[2]!!.value.toLong()) }

        /*
        this is the host that the hints were dropped for
        so if host X drops 100 hints for host Y, we're reporting on Y
         */
        val droppedHintsPerNode = matches.groupingBy { it.first }.fold(0.toLong()) { r, t -> r + t.second }

        val md = MarkdownTable("Host", "Hints Dropped")

        droppedHintsPerNode.forEach {
            md.addRow().addField(it.key).addField(it.value)
        }

        val totalDropped = matches.sumOf { it.second }

        if (totalDropped > 0) {
            recs.immediate(
                RecommendationType.OPERATIONS,"Ensure repairs are running on a regular basis using a tool such as Reaper as " +
                    "dropped hints could be a sign of inconsistency in the cluster.",
                    "Ensure repairs are running regularly as dropped hints were spotted.")
        }

        args["droppedReport"] = md.toString()
        args["totalDropped"] = totalDropped.toString()

		return compileAndExecute("operations/operations_dropped_hints.md", args)
	}
}