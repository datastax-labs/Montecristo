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

import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.schema.Schema
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File

internal class CassandraYamlParserTest {

    @Test
    fun testCassandraFileParse() {

        val yamlFile = File(this.javaClass.getResource("/fileLoaders/parsers/application/cassandra.yaml").path)
        val cassandraYaml = CassandraYamlParser.parse(yamlFile)
        assertThat(cassandraYaml.vNodes).isEqualTo(256)
        assertThat(cassandraYaml.concurrentCompactors).isEqualTo("") // not set
        assertThat(cassandraYaml.memtableAllocationType).isEqualTo("offheap_objects")
        assertThat(cassandraYaml.seeds).isEqualTo("172.23.24.12, 172.23.24.61")
        assertThat(cassandraYaml.lineFor("cluster_name")).isEqualTo(12)
        assertThat(cassandraYaml.lineFor("num_tokens")).isEqualTo(27)
        assertThat(cassandraYaml.lineFor("seed_provider.parameters.seeds")).isEqualTo(422)
        assertThat(cassandraYaml.lineFor("setting_that_does_not_exist")).isNull()
    }

    @Test
    fun testCassandraFileParseRateLimitedLegacyConfigItem() {

        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 50
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest50(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val configRead = cluster.getSetting(ConfigSource.CASS,"compaction_throughput", "64","MiB/s","compaction_throughput_mb_per_sec",  "16", "MB/s")

        assertTrue(configRead.isConsistent())
        assertThat(configRead.values.size).isEqualTo(1)
        assertThat(configRead.values.entries.first().value.isSet).isTrue()
        assertThat(configRead.values.entries.first().value.value.toInt()).isEqualTo(50_000_000)
        assertThat(configRead.values.entries.first().value.units).isEqualTo("MB")
    }

    @Test
    fun testCassandraFileParseRateLegacyAndNewConfigItem() {

        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput: 25 MiB/s
            compaction_throughput_mb_per_sec: 50
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest50(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val configRead = cluster.getSetting(ConfigSource.CASS,"compaction_throughput", "64","MiB/s","compaction_throughput_mb_per_sec",  "16", "MB/s")

        assertTrue(configRead.isConsistent())
        assertThat(configRead.values.size).isEqualTo(1)
        assertThat(configRead.values.entries.first().value.isSet).isTrue()
        assertThat(configRead.values.entries.first().value.value.toInt()).isEqualTo(50_000_000)
        assertThat(configRead.values.entries.first().value.units).isEqualTo("MB")
    }

    @Test
    fun testCassandraFileParseRateLegacyAndNewConfigItemNoUnitSupport() {

        val cassandraYaml = CassandraYamlParser.parse(
            """
            compaction_throughput_mb_per_sec: 50
            compaction_throughput: 25 MiB/s
 
            """.trimIndent()
        )
        val node1 = ObjectCreators.createNode("node1",cassandraYaml = cassandraYaml)
        val nodelist = listOf(node1)
        val schema = mockk<Schema>(relaxed = true)
        val blockedTasks = mockk<BlockedTasks>(relaxed = true)
        val metricServer = mockk<IMetricServer>(relaxed = true)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val configRead = cluster.getSetting(ConfigSource.CASS,"compaction_throughput", "64","MiB/s","compaction_throughput_mb_per_sec",  "16", "MB/s")

        // even though the 25 is the last value, the DB doesn't support that format so it should be ignored.
        assertTrue(configRead.isConsistent())
        assertThat(configRead.values.size).isEqualTo(1)
        assertThat(configRead.values.entries.first().value.isSet).isTrue()
        assertThat(configRead.values.entries.first().value.value.toInt()).isEqualTo(50_000_000)
        assertThat(configRead.values.entries.first().value.units).isEqualTo("MB")
    }
}