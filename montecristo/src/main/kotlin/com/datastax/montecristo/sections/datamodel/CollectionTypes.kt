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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class CollectionTypes : DocumentSection {


    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val md = MarkdownTable("Table", "Maps", "Sets", "Lists")
                .orMessage("""
                    ---
                    
                    _**Noted for reference:** There are no tables using collections._
                    
                    ---
                    
                    """.trimIndent())

        var listCount = 0
        var tableCount = 0
        var tableWithListsCount = 0

        cluster.schema.getUserTables().filter { it.hasNonFrozenCollections()  }.forEach {
            val numLists = it.getLists().count()

            md.addRow().addField(it.name)
                    .addField(it.getMaps().count())
                    .addField(it.getSets().count())
                    .addField(it.getLists().count())

            listCount += numLists
            if (numLists > 0) {
                tableWithListsCount += 1
            }

            tableCount += 1
        }

        if (listCount > 0) {
            val tableSegment = if (tableWithListsCount > 1) "spread across $tableWithListsCount tables" else "in a single table"
            val recSegment: String by lazy {
                if (listCount > 1) {
                    "We found $listCount lists $tableSegment in the schema. For each list, assess"
                } else {
                    "We found 1 list $tableSegment in the schema. Assess"
                }
            }

            recs.near(RecommendationType.DATAMODEL,"We recommend reviewing the use of lists in the data model. $recSegment whether it can be frozen, replaced with a set or map for a list under 100 elements, or with a dedicated table for lists over 100 elements.")
        }


        args["tables_with_collections"] = md.toString()
        if (cluster.schema.getUserTables().any { it.hasNonFrozenCollections() }) {
            args["collections_recs"] =
                    """
                    ---
                    _**Noted for reference**: Fully overwriting a non-frozen collection generates a tombstone. However, when incrementally updating a collection (e.g. adding elements to an existing collection), no tombstone is generated._
                    
                    ---
                    
                    _**Noted for reference**: No tombstone is generated when a frozen collection is fully overwritten._
                    
                    ---

                    """.trimIndent()
            recs.near(RecommendationType.DATAMODEL,"We recommend using a frozen variant of a collection where it is used for immutable data (inserted only once and never updated, or updated as a whole). This is so Cassandra will automatically serialize content into a single binary value. It will remove the overhead of using a collection type in exchange for the ability to perform a partial update of the value.")
        }
        return compileAndExecute("datamodel/datamodel_collections.md", args)
    }
}