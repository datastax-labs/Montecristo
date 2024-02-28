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

package com.datastax.montecristo.sections.infrastructure

import com.google.common.collect.ArrayListMultimap
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation

class Path : DocumentSection {

    private val customParts = StringBuilder()
    private val uniformSwapRecommendation = "We recommend configuring the PATH variable uniformly on each node."
    /**
    OS path is examined:
    - PATH variable from os/env.txt
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // for each path, we'll keep list of nodes where it was seen
        val seenPaths = ArrayListMultimap.create<String, String>()

        cluster.nodes.forEach {node ->
            node.osConfiguration.env.data.forEach {
                if (it.startsWith("PATH")) {
                    seenPaths.put(it, parseNodeName(node.osConfiguration.env.path))
                }
            }
        }

        if (seenPaths.keySet().size == 1 ) {
            args["path-values"] = seenPaths.keySet().first()
            args["path-conclusion"] = "All nodes have the `\$PATH` variable configured in the same way.\n\n"
        } else {
            customParts.append("Count\tPath\n")
            seenPaths.keySet().forEach { path -> customParts.append("${seenPaths.get(path).size}\t$path\n")}
            customParts.append("\n")
            seenPaths.keySet().forEach { path ->
                customParts.append("Path: $path\nNodes: ${seenPaths.get(path)}\n\n")
            }
            args["path-values"] = customParts.toString()
            args["path-conclusion"] = "There are differences in the configuration of the `\$PATH` variable.\n\n$uniformSwapRecommendation\n"
        }
        return compileAndExecute("infrastructure/infrastructure_path.md", args)
    }

    private fun parseNodeName(filePath: String): String {
        val pattern = Regex("extracted/([^_]+)_artifacts_([0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{4})")
        val groups = pattern.find(filePath)?.groups
        return if (!groups.isNullOrEmpty() && !groups[1]?.value.isNullOrEmpty()) {
            groups[1]!!.value
        } else {
            // no node_artifacts_timestamp pattern, extract out the node name
            val pattern = Regex("extracted\\/([^\\/]*)")
            val groups = pattern.find(filePath)?.groups
            // if this fails to find a name, we will state it is an unknown node.
            groups?.get(1)?.value ?: "UNKNOWN_NODE"
        }
    }
}