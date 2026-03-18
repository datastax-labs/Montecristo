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

package com.datastax.montecristo.fileLoaders.parsers.application

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.datastax.montecristo.model.application.CassandraYaml
import java.io.File
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.ScalarNode
import org.yaml.snakeyaml.nodes.SequenceNode

object CassandraYamlParser {

    fun parse(yamlFile: File): CassandraYaml {
        return parse(yamlFile.readText())
    }

    fun parse(data: String): CassandraYaml {
        val yamlReader = ObjectMapper(YAMLFactory())
        val obj = yamlReader.readValue(data, Any::class.java)
        val jsonWriter = ObjectMapper()
        val json = jsonWriter.readTree(jsonWriter.writeValueAsString(obj))
        return CassandraYaml(json, buildLineMap(data))
    }

    private fun buildLineMap(data: String): Map<String, Int> {
        val root = Yaml().compose(data.reader()) ?: return emptyMap()
        val lineNumbers = mutableMapOf<String, Int>()
        walk(root, "", lineNumbers)
        return lineNumbers
    }

    private fun walk(node: Node, path: String, lineNumbers: MutableMap<String, Int>) {
        when (node) {
            is MappingNode -> {
                node.value.forEach { tuple ->
                    val key = (tuple.keyNode as? ScalarNode)?.value ?: return@forEach
                    val childPath = if (path.isEmpty()) key else "$path.$key"
                    lineNumbers.putIfAbsent(childPath, tuple.keyNode.startMark.line + 1)
                    walk(tuple.valueNode, childPath, lineNumbers)
                }
            }
            is SequenceNode -> {
                // Keep path semantics aligned with YamlConfig.getValueFromPath by following first element.
                node.value.firstOrNull()?.let { walk(it, path, lineNumbers) }
            }
            else -> Unit
        }
    }
}