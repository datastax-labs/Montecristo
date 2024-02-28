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

import com.datastax.montecristo.helpers.round
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logger
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class Batches : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val batchWarnings = cluster.databaseVersion.searchLogForBatches(logSearcher, QUERY_LIMIT)

        val numWarnings = if (batchWarnings.size > 1000000) {
            "over a million"
        } else {
            batchWarnings.size.toString()
        }

        args["numWarnings"] = numWarnings
        args["batchesByDay"] = batchWarnings.map { it.getDate().toLocalDate() }.groupBy { it }

        // average size
        val regex = "is of size (\\d*)".toRegex()

        val stats = batchWarnings.map { regex.find(it.message!!)!!.groupValues[1].toLong() }


        if(batchWarnings.isNotEmpty()) {
            try {
                val maxBatch = stats.maxOrNull().toString()
                val avgBatch = stats.average().round()
                args["findingsSummary"] =
                        """
                            There are $numWarnings warnings in the logs regarding large batches.
                            The largest batch we have seen was $maxBatch Kb with an average size of $avgBatch Kb.
                        """.trimIndent()
            } catch (e: Exception) {
                logger.error("Problem with batch documentation, please double check it. $e")
            }
            recs.immediate(RecommendationType.OPERATIONS,"We recommend investigating the cause of large batch warnings in the logs.")
        }
        else {
            args["findingsSummary"] =
                    """
                    ---
            
                    _**Noted for reference**: There are no large batch warnings in the logs._
            
                    ---
                    """.trimIndent()
        }

        return compileAndExecute("operations/operations_batches.md", args)
    }

    companion object {
        private const val QUERY_LIMIT = 1000001
    }
}
