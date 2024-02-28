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

internal class CommitLogSyncTest() {

    @Test
    fun testNoSyncFailure() {

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("PERIODIC-COMMIT-LOG-SYNC OR PERIODIC-COMMIT-LOG-SYNCER", limit = 1000000) } returns emptyList()
        val recs: MutableList<Recommendation> = mutableListOf()
        val commitLog = CommitLogSync()
        val response = commitLog.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 0 commit log sync warnings were discovered within the logs.")
    }

    @Test
    fun testSingleSyncFailure() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "NoSpamLogger.java:94 - Out of 12 commit log syncs over the past 0.00s with average duration of 5026.85ms, 3 have exceeded the configured commit interval by an average of 4026.85ms"

        val logEntry = LogEntry("WARN", example, "20200723184102", "node1")
        entries.add(logEntry)
        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("PERIODIC-COMMIT-LOG-SYNC OR PERIODIC-COMMIT-LOG-SYNCER", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val commitLog = CommitLogSync()
        val response = commitLog.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 1 commit log sync warnings were discovered within the logs.")
        assertThat(response).contains("node1|1|3|4027")
    }

    @Test
    fun testMultiNodeMultiMessageSyncFailure() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example1 = "NoSpamLogger.java:94 - Out of 10 commit log syncs over the past 2.00s with average duration of 1026.85ms, 3 have exceeded the configured commit interval by an average of 1026.85ms"
        val example2 = "NoSpamLogger.java:94 - Out of 11 commit log syncs over the past 3.00s with average duration of 2026.85ms, 4 have exceeded the configured commit interval by an average of 2026.85ms"
        val example3 = "NoSpamLogger.java:94 - Out of 12 commit log syncs over the past 4.00s with average duration of 3026.85ms, 5 have exceeded the configured commit interval by an average of 3026.85ms"
        val example4 = "NoSpamLogger.java:94 - Out of 13 commit log syncs over the past 5.00s with average duration of 4026.85ms, 6 have exceeded the configured commit interval by an average of 4026.85ms"

        entries.add(LogEntry("WARN", example1, "20200723184102", "node1"))
        entries.add(LogEntry("WARN", example2, "20200723184102", "node2"))
        entries.add(LogEntry("WARN", example3, "20200723184102", "node1"))
        entries.add(LogEntry("WARN", example4, "20200723184102", "node2"))

        val cluster = mockk<Cluster>(relaxed = true)
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("PERIODIC-COMMIT-LOG-SYNC OR PERIODIC-COMMIT-LOG-SYNCER", limit = 1000000) } returns entries
        val recs: MutableList<Recommendation> = mutableListOf()
        val commitLog = CommitLogSync()
        val response = commitLog.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("A total of 4 commit log sync warnings were discovered within the logs.")
        assertThat(response).contains("node1|2|8|2027")
        assertThat(response).contains("node2|2|10|3027")
    }
}