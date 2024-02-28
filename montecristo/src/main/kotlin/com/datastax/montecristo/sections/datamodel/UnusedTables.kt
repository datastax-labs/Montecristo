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
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable

class UnusedTables : DocumentSection {


    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        val md = MarkdownTable("Table", "Space Used").orMessage("There are no unused tables.")
        val unused = cluster.schema.getUnusedUserTables().sortedByDescending { it.totalDiskSpaceUsed.count.sum() }

        for(table in unused) {
            md.addRow().addField(table.name).addField(table.totalDiskSpaceUsed.count.sum().humanBytes())
        }


        if(unused.count() > 0) {
            val space = unused.sumOf { it.totalDiskSpaceUsed.count.sum() }.humanBytes()
            recs.immediate(RecommendationType.DATAMODEL,"We recommend investigating ${unused.count()} tables which appear to be unused in queries, but are taking up $space.")
        }

        args["unusedTables"] = md.toString()

        return compileAndExecute("datamodel/datamodel_schema_unused_tables.md", args)
    }
}