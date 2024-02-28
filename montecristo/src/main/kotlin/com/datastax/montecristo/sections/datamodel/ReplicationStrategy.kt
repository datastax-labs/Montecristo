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
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class ReplicationStrategy : DocumentSection {

        override fun getDocument(
            cluster: Cluster,
            logSearcher: Searcher,
            recs: MutableList<Recommendation>,
            executionProfile: ExecutionProfile
        ): String {
                val args = super.createDocArgs(cluster)

                // checking SimpleStrategy usage
                val keyspaces = cluster.schema.keyspaces.sortedBy { it.name }

                val md = MarkdownTable("Keyspace", "Strategy", "Options")
                keyspaces.forEach {
                        md.addRow()
                                .addField(it.name)
                                .addField(it.getStrategyShortName())
                                .addField(it.options.toString())
                }

                args["keyspaces"] = md.toString()

                val (simpleStrategyKs, networkStrategyKeyspace) =
                        keyspaces
                                .partition{ks -> ks.strategy.contains("SimpleStrategy")}

                if (simpleStrategyKs.isNotEmpty()) {
                        recs.near(RecommendationType.DATAMODEL,"We recommend switching the following keyspaces to NetworkTopologyStrategy: ${simpleStrategyKs.joinToString(", ") { it.name }}")
                }

                // check per dc replication factor <= number of nodes
                val nodePerDC = cluster.nodes.groupingBy { node -> node.info.dataCenter }.eachCount()

                // we only filter on NTS keyspaces
                val overReplicatedKs = networkStrategyKeyspace.filter { ks ->
                        // anytime a keyspace is over replicated (more than number of nodes)
                        ks.options.any {
                                it.second > nodePerDC[it.first] ?:0
                        }
                }.map { it.name }.toMutableList()

                // and we filter on SimpleStrategy keyspaces
                overReplicatedKs.addAll(simpleStrategyKs.filter { ks ->
                        ks.options.any {
                                it.second > cluster.nodes.size
                        }
                }.map { it.name })

                if (overReplicatedKs.isNotEmpty()) {
                        recs.immediate(RecommendationType.DATAMODEL,"We recommend reducing the replication factor to be equal or less than the number of nodes for the following keyspaces: ${overReplicatedKs.joinToString(", ")}. Or increasing the number of nodes in the DC to match the replication factor.")
                }

                return compileAndExecute("datamodel/datamodel_replication.md", args)
        }

}