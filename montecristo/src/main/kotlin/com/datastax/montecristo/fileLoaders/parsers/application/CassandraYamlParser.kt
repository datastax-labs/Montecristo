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

object CassandraYamlParser {

    fun parse(yamlFile: File): CassandraYaml {
        return parse(yamlFile.readText())
    }

    fun parse(data: String): CassandraYaml {
        val yamlReader = ObjectMapper(YAMLFactory())
        val obj = yamlReader.readValue(data, Any::class.java)
        val jsonWriter = ObjectMapper()
        val json = jsonWriter.readTree(jsonWriter.writeValueAsString(obj))
        return CassandraYaml(json)
    }
}