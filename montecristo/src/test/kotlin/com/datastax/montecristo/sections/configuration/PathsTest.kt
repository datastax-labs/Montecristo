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
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


internal class PathsTest {

    private val inconsistentTemplate = "\n" +
            "## Paths\n" +
            "\n" +
            "Cassandra stores data in SSTables and the commit log, in multiple paths on the server. The paths are controlled by the `data_file_directories` and `commitlog_directory` settings respectively.\n" +
            "\n" +
            "Data directories in use are inconsistent across the cluster\n" +
            "\n" +
            "```\n" +
            "/var/lib/cassandra/not-default-data : 1 node\n" +
            "/var/lib/cassandra/another-data : 1 node\n" +
            "  \n" +
            "node1 = /var/lib/cassandra/not-default-data\n" +
            "node2 = /var/lib/cassandra/another-data\n" +
            "```\n" +
            "\n" +
            "`commitlog_directory` setting is inconsistent across the cluster: \n" +
            "\n" +
            "```\n" +
            "/var/lib/cassandra/not-default-commitlog : 1 node\n" +
            "/var/lib/cassandra/another-commitlog : 1 node\n" +
            "  \n" +
            "node1 = /var/lib/cassandra/not-default-commitlog\n" +
            "node2 = /var/lib/cassandra/another-commitlog\n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentConsistent() {

        val dataDirectoriesConfigSetting = ConfigurationSetting("data_file_directories", mapOf(Pair("node1", ConfigValue(true, "/var/lib/cassandra/data","/var/lib/cassandra/not-default-data")), Pair("node2", ConfigValue(true, "/var/lib/cassandra/data","/var/lib/cassandra/not-default-data"))))
        val commitLogConfigSetting = ConfigurationSetting("commitlog_directory", mapOf(Pair("node1", ConfigValue(true, "/var/lib/cassandra/commitlog","/var/lib/cassandra/not-default-commitlog")), Pair("node2", ConfigValue(true, "/var/lib/cassandra/commitlog","/var/lib/cassandra/not-default-commitlog"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("data_file_directories", ConfigSource.CASS, "/var/lib/cassandra/data", true) } returns dataDirectoriesConfigSetting
        every { cluster.getSetting("commitlog_directory", ConfigSource.CASS, "/var/lib/cassandra/commitlog", true) } returns commitLogConfigSetting

        val paths = Paths()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = paths.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).doesNotContain("Data directories in use are inconsistent across the cluster") // template is very simple, using contains
    }

    @Test
    fun getDocumentInconsistent() {

        val dataDirectoriesConfigSetting = ConfigurationSetting("data_file_directories", mapOf(Pair("node1", ConfigValue(true, "/var/lib/cassandra/data","/var/lib/cassandra/not-default-data")), Pair("node2", ConfigValue(true, "/var/lib/cassandra/data","/var/lib/cassandra/another-data"))))
        val commitLogConfigSetting = ConfigurationSetting("commitlog_directory", mapOf(Pair("node1", ConfigValue(true, "/var/lib/cassandra/commitlog","/var/lib/cassandra/not-default-commitlog")), Pair("node2", ConfigValue(true, "/var/lib/cassandra/commitlog","/var/lib/cassandra/another-commitlog"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("data_file_directories", ConfigSource.CASS, "/var/lib/cassandra/data", true) } returns dataDirectoriesConfigSetting
        every { cluster.getSetting("commitlog_directory", ConfigSource.CASS, "/var/lib/cassandra/commitlog", true) } returns commitLogConfigSetting
        every { cluster.isDse } returns false
        val paths = Paths()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = paths.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(inconsistentTemplate)
    }

    @Test
    fun getDocumentConsistentList() {

        val dataDirectoriesConfigSetting = ConfigurationSetting("data_file_directories", mapOf(Pair("node1", ConfigValue(true, "/var/lib/cassandra/data","/var/lib/cassandra/not-default-data1,/var/lib/cassandra/not-default-data2"))))
        val commitLogConfigSetting = ConfigurationSetting("commitlog_directory", mapOf(Pair("node1",  ConfigValue(true, "/var/lib/cassandra/commitlog","/var/lib/cassandra/not-default-commitlog"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("data_file_directories", ConfigSource.CASS, "/var/lib/cassandra/data", true) } returns dataDirectoriesConfigSetting
        every { cluster.getSetting("commitlog_directory", ConfigSource.CASS, "/var/lib/cassandra/commitlog", true) } returns commitLogConfigSetting

        val paths = Paths()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = paths.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("- /var/lib/cassandra/not-default-data1")
        assertThat(template).contains("- /var/lib/cassandra/not-default-data2")
        assertThat(template).doesNotContain("Data directories in use are inconsistent across the cluster") // template is very simple, using contains
    }

}