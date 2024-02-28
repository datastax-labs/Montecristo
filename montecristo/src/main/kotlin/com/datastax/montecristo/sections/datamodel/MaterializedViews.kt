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

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.helpers.humanBytes
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class MaterializedViews : DocumentSection {

    private val noMvNoted = "---\n" +
            "\n" +
            "**Noted for reference**: _No materialized views are in use at this time and we recommend that you continue to not use them._\n" +
            "\n" +
            "---"

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val mvTable = MarkdownTable("MV", "Compaction Strategy", "Max Partition Size")
                .orMessage(noMvNoted)
        cluster.schema.tables.filter { it.isMV }.forEach {

            mvTable.addRow()
                    .addField(it.name)
                    .addField(it.compactionStrategy.shortName)
                    .addField(it.maxPartitionSize.value.max()?.humanBytes() ?: "0")
        }
        val numberOfMVs = cluster.schema.tables.filter { it.isMV }.size
        if (numberOfMVs > 0 ) {
            recs.near(RecommendationType.DATAMODEL,"We recommend that a data model review is undertaken to eliminate the use of $numberOfMVs materialized view(s).")
        }
        args["mvtable"] = mvTable.toString()

        return compileAndExecute("datamodel/datamodel_materialized_views.md", args)
    }
}