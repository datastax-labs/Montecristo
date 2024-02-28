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

package com.datastax.montecristo.sections.configuration

import com.datastax.montecristo.helpers.ConfigHelper
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class ConfigurationMismatches : DocumentSection {
    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val configMismatches = MarkdownTable("Node", "Configuration Setting", "Ref Config Value", "Value Found").orMessage("There are no configuration mismatches.")
        val configMissing = MarkdownTable("Node", "Configuration Setting").orMessage("There are no configuration items missing.")

        val args = super.createDocArgs(cluster)
        val (emptyNodes,nonEmptyNodes) = cluster.nodes.partition{it.cassandraYaml.isEmpty()}

        val emptyNodeCount = emptyNodes.size
        if (nonEmptyNodes.size<2) {
            recs.near(RecommendationType.CONFIGURATION,"There are less than two valid cassandra.yaml files, so unable to compare configurations.  The reason for this should be investigated.")
        }
        else {
            val (referenceNode, differences) = ConfigHelper.getConfigMismatches(nonEmptyNodes, ConfigSource.CASS)
            args["referenceNode"] = referenceNode
            var differenceCount = 0
            var missingCount = 0
            var additionalCount = 0

            differences.forEach {
                if (it.value.entriesDiffering().entries.size > 0) {
                    for (difference in it.value.entriesDiffering().entries) {
                        configMismatches.addRow()
                            .addField(it.key)
                            .addField(difference.key)
                            .addField((difference.value.leftValue()?:"").toString())
                            .addField((difference.value.rightValue()?:"").toString())
                        differenceCount++
                    }
                }
            }

            val (_, missingValues) = ConfigHelper.getConfigMissingKeys(nonEmptyNodes, ConfigSource.CASS)
            missingValues.forEach {
                val sb = StringBuilder()
                if (it.value.containsKey("Missing") && it.value.getOrDefault("Missing", emptySet()).isNotEmpty()) {
                    sb.append("Missing: ")
                    it.value["Missing"]!!.forEach { d ->
                        sb.append("$d,")
                        missingCount++
                    }
                }
                val missing = sb.toString().removeSuffix(",")
                if (it.value.containsKey("Additional") && it.value.getOrDefault("Additional", emptySet()).isNotEmpty()) {
                    sb.append("Additional: ")
                    it.value["Additional"]!!.forEach { d ->
                        sb.append("$d,")
                        additionalCount++
                    }
                }
                val additional = sb.toString().removeSuffix(",")
                configMissing.addRow().addField(it.key).addField("$missing   $additional")
            }

            if (differenceCount > 0 || missingCount > 0 || additionalCount > 0) {
                recs.near(RecommendationType.CONFIGURATION,"There are $differenceCount differences, $missingCount missing settings, and $additionalCount additional settings across the nodes in the cluster in the cassandra.yaml. The configuration for the nodes should be aligned to prevent unpredictable behaviour.")
            }
            if (emptyNodeCount > 0) {
                recs.near(RecommendationType.CONFIGURATION,"There are $emptyNodeCount missing cassandra.yaml file(s). The reason for this should be investigated.")
            }
        }

        args["configMismatches"] = configMismatches.toString()
        args["configMissing"] = configMissing.toString()
        return compileAndExecute("configuration/configuration_configuration_mismatches.md", args)

    }
}