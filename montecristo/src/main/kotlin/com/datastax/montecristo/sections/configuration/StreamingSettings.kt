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

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near

class StreamingSettings : DocumentSection {

    /**
    Check the streaming throughput and advise to move back to default if different.
    Check the streaming timeout and advise to set to 1 day if different.
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val customParts = StringBuilder()
        val args = super.createDocArgs(cluster)
        val streamingThroughput = cluster.getSetting("stream_throughput_outbound_megabits_per_sec", ConfigSource.CASS, "200")
        val streamingTimeout = cluster.getSetting("streaming_socket_timeout_in_ms", ConfigSource.CASS, "86400000")

        // Throughput section
        customParts.append(Utils.formatCassandraYamlSetting(streamingThroughput))
        // Timeout section
        customParts.append(Utils.formatCassandraYamlSetting(streamingTimeout))
        args["streaming"] = customParts.toString()

       if (DEFAULT_STREAMING_THROUGHPUT != streamingThroughput.getSingleValue()) {
            recs.near(RecommendationType.CONFIGURATION,streamingThroughputRecommendation)
        }

        if (DEFAULT_STREAMING_TIMEOUT != streamingTimeout.getSingleValue()) {
            recs.near(RecommendationType.CONFIGURATION,streamingTimeoutRecommendation)
        }

        return compileAndExecute("configuration/configuration_streaming.md", args)
    }

    private val streamingThroughputRecommendation = "We recommend setting `stream_throughput_outbound_megabits_per_sec` to its default value of ${DEFAULT_STREAMING_THROUGHPUT}Mb/s as raising it could lead to streaming failures and nodes getting overloaded during repair sessions."
    private val streamingTimeoutRecommendation = "We recommend setting `streaming_socket_timeout_in_ms` to its default of $DEFAULT_STREAMING_TIMEOUT in order to give streams enough time to succeed but prevent sessions from getting stuck forever."

    companion object {
        private const val DEFAULT_STREAMING_THROUGHPUT = "200"
        private const val DEFAULT_STREAMING_TIMEOUT = "86400000"
    }

}
