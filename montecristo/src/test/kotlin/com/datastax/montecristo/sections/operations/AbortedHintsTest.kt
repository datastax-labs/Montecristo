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
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class AbortedHintsTest {


    private val noAbortedHintsTemplate = "## Aborted Hints\n" +
            "\n" +
            "When the Hinted Handoff system attempts to replay mutations the recipient may still fail to process them. When this happens hints are kept on the coordinator and delivery is retried later.  \n" +
            "  \n" +
            "There are no signs of failed hints in the logs.\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n"

    private val abortedHintsTemplate = "## Aborted Hints\n" +
        "\n" +
        "When the Hinted Handoff system attempts to replay mutations the recipient may still fail to process them. When this happens hints are kept on the coordinator and delivery is retried later.  \n" +
        "  \n" +
        "There is 1 log lines indicating failed hints. The most recent ones are:\n" +
        "\n" +
        "```\n" +
        "a log message about a replayed hint\n" +
        "```\n" +
        "\n" +
        "\n" +
        "\n" +
        "\n" +
        "\n"

    @Test
    fun getDocumentNoAbortedHints() {
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("failed AND replaying AND hints") } returns emptyList()


        val recs: MutableList<Recommendation> = mutableListOf()
        val hints = AbortedHints()
        val response = hints.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).isEqualTo(noAbortedHintsTemplate)
    }

    @Test
    fun getDocumentReplayedHints() {
        val entries : MutableList<LogEntry> = mutableListOf()
        val logEntry = LogEntry("WARN", "a log message about a replayed hint", "20200723184102", "node1")
        entries.add(logEntry)
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("failed AND replaying AND hints") } returns entries


        val recs: MutableList<Recommendation> = mutableListOf()
        val hints = AbortedHints()
        val response = hints.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("The presence of aborted hints suggests that the cluster has inconsistencies. We recommend repairing the cluster using Reaper on a regular schedule to reduce entropy.")
        assertThat(response).isEqualTo(abortedHintsTemplate)
    }
}