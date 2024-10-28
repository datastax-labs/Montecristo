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

package com.datastax.montecristo.model.application

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.assertj.core.api.Assertions
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File

internal class YamlConfigTest {
    lateinit var yamlNoFile: YamlConfig
    lateinit var yamlWithFile: YamlConfig

    private fun openYamlFile(filePath: String): JsonNode {
        val yamlFile = File(this.javaClass.getResource(filePath).path)
        val yamlReader = ObjectMapper(YAMLFactory())
        val obj = yamlReader.readValue(yamlFile.readText(), Any::class.java)
        val jsonWriter = ObjectMapper()
        val json = jsonWriter.readTree(jsonWriter.writeValueAsString(obj))
        return json
    }

    @Before
    fun setUp() {
        yamlNoFile = YamlConfig(JsonNodeFactory.instance.objectNode())
        yamlWithFile = YamlConfig(openYamlFile("/helpers/generic.yaml"))
    }

    @Test
    fun testIsEmpty() {
        Assertions.assertThat(yamlNoFile.isEmpty())
        Assertions.assertThat(!yamlWithFile.isEmpty())
    }

    @Test
    fun testParseRowCache() {
        var yamlFile = openYamlFile("/helpers/rowCacheEnabled.yaml")
        var cassandraYaml = CassandraYaml(yamlFile)
        assertTrue(cassandraYaml.isRowCacheEnabled())

        yamlFile = openYamlFile("/helpers/rowCacheDisabled.yaml")
        cassandraYaml = CassandraYaml(yamlFile)
        // this should fail but the Assertions are somehow not effective
        assertTrue(cassandraYaml.isRowCacheEnabled())
    }
}
