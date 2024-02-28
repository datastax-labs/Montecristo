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
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.RecommendationType

class CustomTypes : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        args["types"] = getCustomTypes(cluster.schema.getUserTypes())

        if (!cluster.databaseVersion.isSafeToUseUDT() && cluster.schema.getUserTypes().isNotEmpty()) {
            recs.add(Recommendation(RecommendationPriority.LONG, RecommendationType.DATAMODEL, noUdtRecommendation))
        } else if (cluster.databaseVersion.isSafeToUseUDT() && cluster.schema.getUserTables()
                .any { it.hasNonFrozenTypes() }
        ) {
            recs.add(Recommendation(RecommendationPriority.LONG,RecommendationType.DATAMODEL, noNonFrozenUDTRecommendation))
        }
        return compileAndExecute("datamodel/datamodel_custom_types.md", args)
    }

    private fun getCustomTypes(customTypes: List<String>): String {
        if (customTypes.isEmpty()) {
            return """
				---
				
				_**Noted for Reference:** There are no custom types in use._
				
				---
				
				""".trimIndent()
        }
        val output = StringBuilder()

        output.append("""```
            |${customTypes.joinToString("\n") { it.substringBefore("(") }}
            |```
            |
            """.trimMargin("|"))
        return output.toString()
    }

    private val noUdtRecommendation = "We advise against using UDTs before Cassandra 3.11 due to upgrade bugs affecting 3.0 SSTables."
    private val noNonFrozenUDTRecommendation = "We advise against using non-frozen UDTs for performance reasons. Latencies and heap pressure will be much lower when using a json representation of the objects stored as a text field"
}