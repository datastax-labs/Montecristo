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
import com.datastax.montecristo.sections.structure.RecommendationPriority
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class BatchesTest {

    private val templateSingleWarning = "## Batches\n" +
            "\n" +
            "Batches can be used to group together queries.  Batches provide a guarantee all are eventually executed, but do not provide the semantics of a relational database such as atomicity (all or nothing) or isolation. They are most useful for queries on related tables (such as multiple views of the same data) but come at a significant overhead. We recommend limiting the usage of batches to only a few queries.\n" +
            "\n" +
            "Large batches can cause significant GC pauses and result in cluster instability.  In the case of a single query in a batch failing, the entire batch will fail and must be retried.\n" +
            "\n" +
            "In the case of large clusters, two unavailable nodes can cause an entire batch to fail, resulting in a rapidly growing batch log and hint pileup.  This can lead to a snowball effect resulting in additional downtime.  \n" +
            "\n" +
            "Single partition batches do not have the overhead of multi-partition batches. They do not require the batch log and are executed atomically and in isolation as a single mutation.\n" +
            "\n" +
            "There are 1 warnings in the logs regarding large batches.\n" +
            "The largest batch we have seen was 100 Kb with an average size of 100.0 Kb.\n"

    private val templateMillionWarning = "## Batches\n" +
            "\n" +
            "Batches can be used to group together queries.  Batches provide a guarantee all are eventually executed, but do not provide the semantics of a relational database such as atomicity (all or nothing) or isolation. They are most useful for queries on related tables (such as multiple views of the same data) but come at a significant overhead. We recommend limiting the usage of batches to only a few queries.\n" +
            "\n" +
            "Large batches can cause significant GC pauses and result in cluster instability.  In the case of a single query in a batch failing, the entire batch will fail and must be retried.\n" +
            "\n" +
            "In the case of large clusters, two unavailable nodes can cause an entire batch to fail, resulting in a rapidly growing batch log and hint pileup.  This can lead to a snowball effect resulting in additional downtime.  \n" +
            "\n" +
            "Single partition batches do not have the overhead of multi-partition batches. They do not require the batch log and are executed atomically and in isolation as a single mutation.\n" +
            "\n" +
            "There are over 1000000 warnings in the logs regarding large batches.\n" +
            "The largest batch we have seen was 100 Kb with an average size of 100.0 Kb.\n"

    @Test
    fun getDocumentSingleBatchWarning() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("ERROR", "Test batch message is of size 100 KB", "20200723184102", "node1")
        entries.add(logEntry)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion.searchLogForBatches(searcher, 1000001) } returns entries

        val batches = Batches()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = batches.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        Assertions.assertThat(recs.first().longForm).isEqualTo("We recommend investigating the cause of large batch warnings in the logs.")

       Assertions.assertThat(template).isEqualTo(templateSingleWarning)
    }

    @Test
    fun getDocumentOver1MillionWarnings() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("ERROR", "Test batch message is of size 100 KB", "20200723184102", "node1")
        for (x in 1..1000001) {
            entries.add(logEntry)
        }
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion.searchLogForBatches(searcher, 1000001) } returns entries

        val batches = Batches()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = batches.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(1)
        Assertions.assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        Assertions.assertThat(recs.first().longForm).isEqualTo("We recommend investigating the cause of large batch warnings in the logs.")

        Assertions.assertThat(template).isEqualTo(templateMillionWarning)
    }
}