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

class AbortedHints : DocumentSection {

    private val customParts = StringBuilder()

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val failedHintsLogs = logSearcher.search("failed AND replaying AND hints")

        // count all lines
        val failedHintsCount = failedHintsLogs.count()

        // most recent 10 lines
        val last10 = failedHintsLogs.takeLast(10)

        if (failedHintsCount == 0) {
            customParts.append("There are no signs of failed hints in the logs.\n")
        } else {
            customParts.append("There is $failedHintsCount log lines indicating failed hints. The most recent ones are:\n\n")
            customParts.append("```\n")
            last10.forEach { customParts.append("${it.message}\n") }
            customParts.append("```\n\n")
            recs.immediate(RecommendationType.OPERATIONS,"The presence of aborted hints suggests that the cluster has inconsistencies. We recommend repairing the cluster using Reaper on a regular schedule to reduce entropy.")
        }
        args["failedHints"] = customParts.toString()
        return compileAndExecute("operations/operations_aborted_hints.md", args)
    }
}