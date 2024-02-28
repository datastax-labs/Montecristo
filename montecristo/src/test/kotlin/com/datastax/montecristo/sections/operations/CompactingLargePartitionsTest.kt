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
import com.datastax.montecristo.logs.LogRegex
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert
import org.junit.Test

class CompactingLargePartitionsTest {

    @Test
    fun testNoLargePartitions() {

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("large partition", LogLevel.WARN, 1000000) } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val partitions = CompactingLargePartitions()
        val response = partitions.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("No large partition warnings have been found within the logs.")
        assertThat(response).contains("No partitions over 100 MB have been tracked.")
    }

    @Test
    fun testSingleWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "WARN  [CompactionExecutor:787] 2017-11-27 15:46:46,132 SSTableWriter.java:240 - Compacting large partition myKeyspace/myTable:<primary_key> (148802470 bytes)"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val logEntry = LogEntry.fromString(example, regex)
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+large +partition", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val partitions = CompactingLargePartitions()
        val response = partitions.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued - seems strange
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("myKeyspace.myTable|1")
        assertThat(response).contains("2017-11-27|1")
    }

    @Test
    fun testMultipleWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val entry1 = "WARN  [CompactionExecutor:787] 2017-11-27 15:46:46,132 SSTableWriter.java:240 - Compacting large partition myKeyspace/myTable:<primary_key> (148802470 bytes)"
        val entry2 = "WARN  [CompactionExecutor:787] 2017-11-28 15:46:46,132 SSTableWriter.java:240 - Compacting large partition myKeyspace/myTable:<primary_key> (148802470 bytes)"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        entries.add(LogEntry.fromString(entry1, regex))
        entries.add(LogEntry.fromString(entry2, regex))

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+large +partition", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val partitions = CompactingLargePartitions()
        val response = partitions.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued - seems strange
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("myKeyspace.myTable|2")
        assertThat(response).contains("2017-11-27|1")
        assertThat(response).contains("2017-11-28|1")
    }

    @Test
    fun testSingleRepairHistoryWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "WARN  [CompactionExecutor:787] 2017-11-27 15:46:46,132 SSTableWriter.java:240 - Compacting large partition system_distributed/repair_history:<primary_key> (148802470 bytes)"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val logEntry = LogEntry.fromString(example, regex)
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+large +partition", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val partitions = CompactingLargePartitions()
        val response = partitions.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued - seems strange
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend truncating the contents of the system_distributed.repair_history table when the repair process is not running, to eliminate the large partitions within the table.")
        assertThat(response).contains("system_distributed.repair_history|1")
        assertThat(response).contains("2017-11-27|1")
    }

    @Test
    fun testParseLargePartitionPost311() {
        val exampleLogLine = "WARN  [CompactionExecutor:30354] 2020-05-14 17:04:56,008 BigTableWriter.java:211 - Writing large partition ks/table:partition (180.283MiB) to sstable /var/lib/cassandra/data/ks/table-7527f320b7ca11e9b941b7ed10b07722/.index/md-87411-big-Data.db"
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val largePartitions = CompactingLargePartitions.getSortedPartitionSizes(cluster,listOf(exampleLogLine))
        Assert.assertFalse(largePartitions.isEmpty())
        Assert.assertEquals(189040427, largePartitions.get(0).second)
    }

    @Test
    fun testParseLargePartitionPre311() {
        val exampleLogLine = "WARN  [CompactionExecutor:156261] 2019-12-23 09:56:52,207 SSTableWriter.java:240 - Compacting large partition kd/table:partition (352450669 bytes)"
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.0.0")
        val largePartitions = CompactingLargePartitions.getSortedPartitionSizes(cluster,listOf(exampleLogLine))
        Assert.assertFalse(largePartitions.isEmpty())
        Assert.assertEquals(352450669, largePartitions.get(0).second)
    }

    @Test
    fun testParseLargePartitionPre21() {
        val exampleLogLine = "INFO [CompactionExecutor:206279] 2020-05-05 08:00:02,512 CompactionController.java (line 192) Compacting large row laspaf/user_interaction_log_headers.user_interaction_log_header_status_idx:0 (119727385 bytes) incrementally"
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("2.0.0")
        val largePartitions = CompactingLargePartitions.getSortedPartitionSizes(cluster,listOf(exampleLogLine))
        Assert.assertFalse(largePartitions.isEmpty())
        Assert.assertEquals(119727385, largePartitions.get(0).second)
    }

    @Test
    fun testCountByDate() {
        val exampleLogLine = "INFO [CompactionExecutor:206279] 2020-05-05 08:00:02,512 CompactionController.java (line 192) Compacting large row laspaf/user_interaction_log_headers.user_interaction_log_header_status_idx:0 (119727385 bytes) incrementally"
        val logEntry = LogEntry("WARN", exampleLogLine, "20200505080002")
        val largePartitions = CompactingLargePartitions.countByDate(listOf(logEntry))
        Assert.assertFalse(largePartitions.isEmpty())
        Assert.assertEquals("2020-05-05", largePartitions.first().first )
    }
}