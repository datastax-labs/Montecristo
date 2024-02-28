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
import java.io.File

internal class CassandraYamlParserTest {

    @Test
    fun testCassandraFileParse() {

        val yamlFile = File(this.javaClass.getResource("/fileLoaders/parsers/application/cassandra.yaml").path)
        val cassandraYaml = CassandraYamlParser.parse(yamlFile)
        assertThat(cassandraYaml.vNodes).isEqualTo (256)
        assertThat(cassandraYaml.compactionThroughput).isEqualTo ("0")
        assertThat(cassandraYaml.concurrentCompactors).isEqualTo ("") // not set
        assertThat(cassandraYaml.memtableAllocationType).isEqualTo("offheap_objects")
        assertThat(cassandraYaml.seeds).isEqualTo("172.23.24.12, 172.23.24.61")
    }

}