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
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable

class SchemaVersions : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val md = MarkdownTable("Node", "Schema Version")

        val schemIsInAgreement = if (!cluster.schema.isSchemaInAgreement) {
            // the schema agreement / disagreement is calculated based off of the hash values of the cql files, not describe cluster output.
            // if the CQL disagreed, double check the gossip schema schema uuid, for more than 1 values
            // more than 1 schema guid and we do have genuine disagreement, if all the values are the same, we are seeing the effect of a schema change between nodes being collected.
            cluster.nodes.first().gossipInfo.map { it.value.schema}.toSet().size <= 1
        } else {
            true
        }

        args["agreement"] = schemIsInAgreement
        if (!schemIsInAgreement) {
            recs.immediate(RecommendationType.DATAMODEL,"We recommend you engage the DataStax Services team further to determine the cause of schema disagreement within the cluster. This can be caused by frequent schema updates. Schema disagreement that do not resolve over time can result in data loss and degraded availability.")

            cluster.nodes.first().gossipInfo.forEach {
                md.addRow()
                    .addField(it.key) // node
                    .addField(it.value.schema) // schema uuid
            }
        }
        args["schemaVersions"] = md.toString()

        return compileAndExecute("datamodel/datamodel_schema_agreement.md", args)
    }
}