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

class SecondaryIndexes : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val md = MarkdownTable("Index", "Table", "Column").orMessage("There are no secondary indexes in the data model.")
        val secondaryIndexes = cluster.schema.getUserIndexes()
        if (secondaryIndexes.isNotEmpty()) {
            val thereAreNb2i = if (secondaryIndexes.size == 1)  "There is one secondary index" else "There are ${secondaryIndexes.size} secondary indexes"
            recs.near(RecommendationType.DATAMODEL,"We recommend reviewing the model of tables using secondary indexes. $thereAreNb2i in the data model.")
            secondaryIndexes.forEach {
                md.addRow().addField(it.indexName).addField("${it.getKeyspace()}.${it.getTableName()}").addField(it.partitionKeys.first())
            }
            args["2i_in_use"] = md.toString()
        }
        return compileAndExecute("datamodel/datamodel_secondary_indexes.md", args)
    }
}