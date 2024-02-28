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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class LogAnalysisDurationTest {

    @Test
    fun testLogDurations() {

        val durationMap = mutableMapOf<String,Pair<String,String>>()
        durationMap.put("node1",Pair("2000-01-01T00:00:00", "2000-01-15T12:13:14"))
        durationMap.put("node2",Pair("2000-01-02T12:00:00", "2000-04-01T01:02:03"))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.metricServer.getLogDurations() } returns durationMap
        every { cluster.getNode("node1")?.info?.getUptimeInDays() } returns "1.00"
        every { cluster.getNode("node2")?.info?.getUptimeInDays() } returns "2.00"

        val recs: MutableList<Recommendation> = mutableListOf()
        val logAnalysisDuration = LogAnalysisDuration()
        val result = logAnalysisDuration.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(result).contains("node1|2000-01-01T00:00:00|2000-01-15T12:13:14|14.50|1.00")
        assertThat(result).contains("node2|2000-01-02T12:00:00|2000-04-01T01:02:03|89.54|2.00")
    }
}
