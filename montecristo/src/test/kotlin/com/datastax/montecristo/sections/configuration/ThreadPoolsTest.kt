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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class ThreadPoolsTest {

    private val consistentTemplate = "## Thread Pools\n" +
            "\n" +
            "Cassandra uses a [Staged Event Driven Architecture](https://en.wikipedia.org/wiki/Staged_event-driven_architecture) that uses thread pools for common tasks. The read and the write paths have their own thread pools, where the number of threads can be controlled through configuration. Typically the number of threads is increased to put more pressure on the storage system and allow it to re-order commands in the most efficient way possible. The size of the read and write thread pools are controlled by the `concurrent_reads`, `concurrent_writes` `concurrent_counter_writes`, `concurrent_batchlog_writes` and `concurrent_materialized_view_writes` configuration settings.\n" +
            "  \n" +
            "\n" +
            "`concurrent_reads` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "concurrent_reads: 32 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "`concurrent_writes` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "concurrent_writes: 64 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "`concurrent_counter_writes` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "concurrent_counter_writes: 128 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentConsistent() {

        val readsConfigSetting = ConfigurationSetting("concurrent_reads", mapOf(Pair("node1", ConfigValue(true, "","32")), Pair("node2", ConfigValue(true, "","32"))))
        val writesConfigSetting = ConfigurationSetting("concurrent_writes", mapOf(Pair("node1", ConfigValue(true, "","64")), Pair("node2", ConfigValue(true, "","64"))))
        val counterConfigSetting = ConfigurationSetting("concurrent_counter_writes", mapOf(Pair("node1", ConfigValue(true, "","128")), Pair("node2", ConfigValue(true, "","128"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns false
        every { cluster.databaseVersion} returns DatabaseVersion.fromString("3.11.9")
        every { cluster.getSetting("concurrent_reads", ConfigSource.CASS) } returns readsConfigSetting
        every { cluster.getSetting("concurrent_writes", ConfigSource.CASS) } returns writesConfigSetting
        every { cluster.getSetting("concurrent_counter_writes", ConfigSource.CASS) } returns counterConfigSetting

        val tp = ThreadPools()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tp.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(consistentTemplate)
    }


    @Test
    fun getDocumentDSE5() {

        val readsConfigSetting = ConfigurationSetting("concurrent_reads", mapOf(Pair("node1", ConfigValue(true, "","32")), Pair("node2", ConfigValue(true, "","32"))))
        val writesConfigSetting = ConfigurationSetting("concurrent_writes", mapOf(Pair("node1", ConfigValue(true, "","64")), Pair("node2", ConfigValue(true, "","64"))))
        val counterConfigSetting = ConfigurationSetting("concurrent_counter_writes", mapOf(Pair("node1", ConfigValue(true, "","128")), Pair("node2", ConfigValue(true, "","128"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion} returns DatabaseVersion.fromString("5.1.18", true)
        every { cluster.getSetting("concurrent_reads", ConfigSource.CASS) } returns readsConfigSetting
        every { cluster.getSetting("concurrent_writes", ConfigSource.CASS) } returns writesConfigSetting
        every { cluster.getSetting("concurrent_counter_writes", ConfigSource.CASS) } returns counterConfigSetting

        val tp = ThreadPools()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tp.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo(consistentTemplate)
    }

    @Test
    fun getDocumentDSE6() {

        val readsConfigSetting = ConfigurationSetting("concurrent_reads", mapOf(Pair("node1", ConfigValue(true, "","32")), Pair("node2", ConfigValue(true, "","32"))))
        val writesConfigSetting = ConfigurationSetting("concurrent_writes", mapOf(Pair("node1", ConfigValue(true, "","64")), Pair("node2",ConfigValue(true, "", "64"))))
        val counterConfigSetting = ConfigurationSetting("concurrent_counter_writes", mapOf(Pair("node1", ConfigValue(true, "","128")), Pair("node2", ConfigValue(true, "","128"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.isDse } returns true
        every { cluster.databaseVersion} returns DatabaseVersion.fromString("6.8.13", true)
        every { cluster.getSetting("concurrent_reads", ConfigSource.CASS) } returns readsConfigSetting
        every { cluster.getSetting("concurrent_writes", ConfigSource.CASS) } returns writesConfigSetting
        every { cluster.getSetting("concurrent_counter_writes", ConfigSource.CASS) } returns counterConfigSetting

        val tp = ThreadPools()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = tp.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(template).isEqualTo("")
    }
}