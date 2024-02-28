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

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class JvmGcStatsTest {

    private val noPauseTemplate = "## JVM Garbage Collection\n" +
            "\n" +
            "There are two valid options for garbage collection with Cassandra:\n" +
            "\n" +
            "* **Par New + CMS**: The default garbage collector algorithms.  They can be optimized for low latency and high throughput but require a deep understanding of them and can be tricky to tune.\n" +
            "* **G1GC**: A collector optimized for high throughput, with minimal configuration, but typically displays higher latencies.\n" +
            "\n" +
            "The overall distribution of pause times is as follows:\n" +
            "\n" +
            "\n" +
            "\n" +
            "Pause times by day, broken down by time spent:\n" +
            "\n" +
            "\n" +
            "\n"

    @Test
    fun testNoGCPauses() {

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("GCInspector.java") } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val jvmgc = JvmGcStats()
        val response = jvmgc.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).isEqualTo(noPauseTemplate)
    }

    @Test
    fun testSinglePause() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "GCInspector.java:284 - ParNew GC in 237ms.  CMS Old Gen: 6661992176 -> 6706569168; Par Eden Space: 671088640 -> 0; Par Survivor Space: 64102592 -> 66356720"

        val logEntry = LogEntry("WARN", example, "20200723184102", "node1")
        entries.add(logEntry)
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("GCInspector.java", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val jvmgc = JvmGcStats()
        val response = jvmgc.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("<span><div style=\"text-align:left;\">Range</div></span>|<span><div style=\"text-align:left;\">Count</div></span>\n" +
                "---|---\n" +
                "0.2 - 0.3 s|1\n")
        assertThat(response).contains("<span><div style=\"text-align:left;\">Day</div></span>|<span><div style=\"text-align:left;\">GC Pauses > 200ms</div></span>|<span><div style=\"text-align:left;\">GC Pauses > 1s</div></span>\n" +
                "---|---|---\n" +
                "2020-07-23|1|0\n")
    }

}