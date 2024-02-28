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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class DseYamlParserTest() {
    @Test
    fun testDseFileParse() {
        val yamlFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/application/dse.yaml").reader().readText()
        val dseYaml = DseYamlParser.parse(yamlFile)
        assertThat(dseYaml.get("enable_health_based_routing")).isEqualTo("true")
    }
}