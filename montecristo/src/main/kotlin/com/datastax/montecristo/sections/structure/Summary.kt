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

package com.datastax.montecristo.sections.structure

import com.datastax.montecristo.helpers.stripHTMLComments
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.utils.MarkdownTable

class Summary(private val immediate: List<Recommendation>, private var nearTerm: List<Recommendation>, private val longTerm: List<Recommendation>) : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        // group by section / error
        val errorsGroupedBySection = cluster.loadErrors.groupBy { it.error }

        val loadErrorsTable = MarkdownTable("Count", "Node", "Issue").orMessage("There were no errors loading the data collected")

        errorsGroupedBySection.forEach{
            loadErrorsTable.addRow()
                    .addField(it.value.size)
                    .addField(it.value.joinToString(separator = ",") { n-> n.node } )
                    .addField(it.value.first().error)
        }
        val logStats = cluster.metricServer.getLogStats()
         logStats.filter { it.value.second != "0"}.forEach {
            loadErrorsTable.addRow()
                .addField("-")
                .addField(it.key)
                .addField("${it.value.second} log entries were skipped, ${it.value.first} entries were successfully parsed.")
         }

        val args = super.createDocArgs(cluster)
        args["immediate-infra"] = immediate.filter { it.type == RecommendationType.INFRASTRUCTURE }.map { it.shortForm.stripHTMLComments() }
        args["immediate-configuration"] = immediate.filter { it.type == RecommendationType.CONFIGURATION }.map { it.shortForm.stripHTMLComments() }
        args["immediate-security"] = immediate.filter { it.type == RecommendationType.SECURITY }.map { it.shortForm.stripHTMLComments() }
        args["immediate-datamodel"] = immediate.filter { it.type == RecommendationType.DATAMODEL }.map { it.shortForm.stripHTMLComments() }
        args["immediate-operations"] = immediate.filter { it.type == RecommendationType.OPERATIONS }.map { it.shortForm.stripHTMLComments() }
        args["immediate-misc"] = immediate.filter { it.type == RecommendationType.UNCLASSIFIED }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-infra"] = nearTerm.filter { it.type == RecommendationType.INFRASTRUCTURE }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-configuration"] = nearTerm.filter { it.type == RecommendationType.CONFIGURATION }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-security"] = nearTerm.filter { it.type == RecommendationType.SECURITY }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-datamodel"] = nearTerm.filter { it.type == RecommendationType.DATAMODEL }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-operations"] = nearTerm.filter { it.type == RecommendationType.OPERATIONS }.map { it.shortForm.stripHTMLComments() }
        args["nearterm-misc"] = nearTerm.filter { it.type == RecommendationType.UNCLASSIFIED }.map { it.shortForm.stripHTMLComments() }
        args["longterm-infra"] = longTerm.filter { it.type == RecommendationType.INFRASTRUCTURE }.map { it.shortForm.stripHTMLComments() }
        args["longterm-configuration"] = longTerm.filter { it.type == RecommendationType.CONFIGURATION }.map { it.shortForm.stripHTMLComments() }
        args["longterm-security"] = longTerm.filter { it.type == RecommendationType.SECURITY }.map { it.shortForm.stripHTMLComments() }
        args["longterm-datamodel"] = longTerm.filter { it.type == RecommendationType.DATAMODEL }.map { it.shortForm.stripHTMLComments() }
        args["longterm-operations"] = longTerm.filter { it.type == RecommendationType.OPERATIONS }.map { it.shortForm.stripHTMLComments() }
        args["longterm-misc"] = longTerm.filter { it.type == RecommendationType.UNCLASSIFIED }.map { it.shortForm.stripHTMLComments() }

        args["loadErrors"] = loadErrorsTable.toString()
        return compileAndExecute("structure/summary.md", args)

    }
}