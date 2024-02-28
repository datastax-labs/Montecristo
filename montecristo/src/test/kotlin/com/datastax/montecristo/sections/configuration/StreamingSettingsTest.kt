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

internal class StreamingSettingsTest {

    private val defaultTemplate = "## Streaming\n" +
            "\n" +
            "Streaming is the process Cassandra uses during repair, rebuild and bootstrapping to transfer sections of SSTables between nodes. Large chunks of files can be efficiently streamed between nodes, avoiding the read and write API. There are several settings that control streaming in the configuration.\n" +
            "\n" +
            "The `streaming_socket_timeout_in_ms` setting specifies how long to wait before timing out. The default in versions 1.2 and 2.0 in use is 0, which disables the timeout. With this in place nodes must be restarted to clear failed streaming sessions.\n" +
            "\n" +
            "`stream_throughput_outbound_megabits_per_sec` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "stream_throughput_outbound_megabits_per_sec: 200 \n" +
            "```\n" +
            "\n" +
            "`streaming_socket_timeout_in_ms` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "streaming_socket_timeout_in_ms: 86400000 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    private val nonDefaultTemplate = "## Streaming\n" +
            "\n" +
            "Streaming is the process Cassandra uses during repair, rebuild and bootstrapping to transfer sections of SSTables between nodes. Large chunks of files can be efficiently streamed between nodes, avoiding the read and write API. There are several settings that control streaming in the configuration.\n" +
            "\n" +
            "The `streaming_socket_timeout_in_ms` setting specifies how long to wait before timing out. The default in versions 1.2 and 2.0 in use is 0, which disables the timeout. With this in place nodes must be restarted to clear failed streaming sessions.\n" +
            "\n" +
            "`stream_throughput_outbound_megabits_per_sec` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "stream_throughput_outbound_megabits_per_sec: 500 \n" +
            "```\n" +
            "\n" +
            "`streaming_socket_timeout_in_ms` is configured as follows: \n" +
            "\n" +
            "```\n" +
            "streaming_socket_timeout_in_ms: 250000 \n" +
            "```\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentDefault() {

        val throughputConfigSetting = ConfigurationSetting("stream_throughput_outbound_megabits_per_sec", mapOf(Pair("node1", ConfigValue(false, "200","")), Pair("node2",ConfigValue(false, "200",""))))
        val timeoutConfigSetting = ConfigurationSetting("streaming_socket_timeout_in_ms", mapOf(Pair("node1",ConfigValue(false, "86400000","")), Pair("node2",ConfigValue(false, "86400000",""))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("stream_throughput_outbound_megabits_per_sec",  ConfigSource.CASS ,"200") } returns throughputConfigSetting
        every { cluster.getSetting("streaming_socket_timeout_in_ms",  ConfigSource.CASS ,"86400000") } returns timeoutConfigSetting
        every { cluster.isDse } returns false

        val streaming = StreamingSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = streaming.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(defaultTemplate)
    }

    @Test
    fun getDocumentNonDefault() {

        val throughputConfigSetting = ConfigurationSetting("stream_throughput_outbound_megabits_per_sec", mapOf(Pair("node1",ConfigValue(true, "200","500")), Pair("node2",ConfigValue(true, "200","500"))))
        val timeoutConfigSetting = ConfigurationSetting("streaming_socket_timeout_in_ms", mapOf(Pair("node1",ConfigValue(true, "86400000","250000")), Pair("node2",ConfigValue(true, "86400000","250000"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("stream_throughput_outbound_megabits_per_sec",  ConfigSource.CASS ,"200") } returns throughputConfigSetting
        every { cluster.getSetting("streaming_socket_timeout_in_ms",  ConfigSource.CASS ,"86400000") } returns timeoutConfigSetting

        val streaming = StreamingSettings()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = streaming.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(2)

        assertThat(recs[0].longForm).isEqualTo("We recommend setting `stream_throughput_outbound_megabits_per_sec` to its default value of 200Mb/s as raising it could lead to streaming failures and nodes getting overloaded during repair sessions.")
        assertThat(recs[1].longForm).isEqualTo("We recommend setting `streaming_socket_timeout_in_ms` to its default of 86400000 in order to give streams enough time to succeed but prevent sessions from getting stuck forever.")
        assertThat(template).isEqualTo(nonDefaultTemplate)
    }
}