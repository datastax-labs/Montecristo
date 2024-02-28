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

package com.datastax.montecristo.helpers

import com.google.common.collect.MapDifference
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.Node

object ConfigHelper {

    // Return value a pair of <referenceNode, differences found>
    fun getConfigMismatches(nodes: List<Node>, configSource: ConfigSource): Pair<String, Map<String, MapDifference<String, Any>>> {

        val nodeYaml = nodes.map {
            if (configSource == ConfigSource.CASS) {
                Pair(it.hostname, it.cassandraYaml.asMap())
            } else {
                Pair(it.hostname, it.dseYaml.asMap())
            }
        }.toList()

        val configMismatches = mutableMapOf<String, MapDifference<String, Any>>()
        // the first node is arbitrarily considered the 'reference', and compared to all other nodes.
        val (referenceNode, referenceNodeData) = nodeYaml.first()
        val referenceWithoutAddresses = removeNodeAndClusterSpecificValues(referenceNodeData)
        val otherNodes = nodeYaml.subList(1, nodeYaml.size)
        otherNodes.forEach { node ->
            val (hostname, data) = node
            val valuesWithoutAddresses = removeNodeAndClusterSpecificValues(data)
            val diff = Maps.difference(referenceWithoutAddresses, valuesWithoutAddresses)
            if (!diff.areEqual()) {
                configMismatches[hostname] = diff
            }
        }
        return Pair(referenceNode, configMismatches.toMap())
    }

    fun getConfigMissingKeys(nodes: List<Node>, configSource: ConfigSource): Pair<String,Map<String, Map<String, Sets.SetView<String>>>> {

        val nodeYaml = nodes.map {
            if (configSource == ConfigSource.CASS) {
                Pair(it.hostname, it.cassandraYaml.asMap())
            } else {
                Pair(it.hostname, it.dseYaml.asMap())
            }
        }.toList()

        val configMissingKeys = mutableMapOf<String, Map<String, Sets.SetView<String>>>()
        // the first node is arbitrarily considered the 'reference', and compared to all other nodes.
        val (referenceNode, referenceNodeData) = nodeYaml.first()

        val otherNodes = nodeYaml.subList(1, nodeYaml.size)
        otherNodes.forEach { node ->
            val (hostname, data) = node
            // Find keys that do not exist in both configurations
            val missingKeys = Sets.difference(referenceNodeData.keys, data.keys)
            // Find keys that exist in the non-reference nodes - but not the references (thus considered additional)
            val additionalKeys = Sets.difference(data.keys, referenceNodeData.keys)
            val keyDiffs = Maps.newHashMap<String, Sets.SetView<String>>()
            keyDiffs["Missing"] = missingKeys
            keyDiffs["Additional"] = additionalKeys
            if (missingKeys.size > 0 || additionalKeys.size > 0) {
                configMissingKeys[hostname] = keyDiffs
            }
        }
        return Pair(referenceNode, configMissingKeys.toMap())
    }

    fun removeNodeAndClusterSpecificValues(config: Map<String, Any>): Map<String, Any> {
        val tmpConfig = Maps.newHashMap<String, Any>()
        config.keys
                .filter { k -> k !in arrayOf("listen_address", "broadcast_address", "rpc_address", "initial_token", "broadcast_rpc_address", "seed_provider", "cluster_name","native_transport_address", "native_transport_interface", "native_transport_broadcast_address", "initial_token") }
                .forEach { key -> tmpConfig[key] = config[key] }
        return tmpConfig
    }
}