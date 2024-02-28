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
import org.junit.Test

class TombstoneWarningsTest {

    private val noWarningsTemplate = "## Tombstone Warnings\n" +
            "\n" +
            "When Cassandra receives a delete request it appends a special value to the data to be deleted, called a tombstone. A tombstone is a marker which indicates that the preceding data is to be considered deleted. A tombstone is also written if a TTL set on the data cell expires. The tombstone will persist for the period of time defined by `gc_grace_seconds`. This is a setting that each table in Cassandra has. After the `gc_grace_seconds` period the tombstone expires.\n" +
            "\n" +
            "Prior to the tombstone expiry, if the associated data is compacted, the compaction process will drop the data when it writes out the new table data to disk. The compaction process will still persist the tombstone and only it will be written out to the new table data on disk. After a tombstone expires, it can potentially be dropped the next time the SSTable is compacted. Older data marked for deletion can be spread across multiple tables. Those SSTables that make up the older data must all be part of the compaction for the tombstone to be dropped. If one or more of those SSTables that forms the delete data is left out of the compaction, then the tombstone will remain.\n" +
            "\n" +
            "If no compaction occurs during the period between the tombstone creation and expiry, both the data and expired tombstone will still exist. In this case, during the next compaction both the data marked for deletion and the associated tombstone could potentially be dropped. Once again the SSTables that make up the older data must all be part of the compaction for the tombstone and data to be dropped.\n" +
            "\n" +
            "The tombstone expiry delay introduced by `gc_grace_seconds` is done to allow the consistency mechanisms to propagate the delete operation among replicas holding the given data. Using this process, data can eventually be reliably deleted from all replicas in the cluster.\n" +
            "\n" +
            "When reading data from disk, Cassandra reads every value for the requested cell in order to pick the most recent one. This involves reading potentially an excessive number of tombstones which might cause memory pressure or even exhaust all memory available. To indicate a danger of this happening, Cassandra issues warning log lines.\n" +
            "\n" +
            "There were no tombstone log messages detected."

    @Test
    fun testNoTombstones() {

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+live +tombstone", LogLevel.WARN, 1000000) } returns emptyList()

        val recs: MutableList<Recommendation> = mutableListOf()
        val tombstones = TombstoneWarnings()
        val response = tombstones.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).isEqualTo(noWarningsTemplate)
    }

    @Test
    fun testSingleWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "WARN  [SharedPool-Worker-1] 2020-09-03 10:25:03,316 ReadCommand.java:520 - Read 61 live rows and 2968 tombstone cells for query SELECT * FROM maf.timeseries_index WHERE systemid, bucket, partition = 30001, 159846, 0702052a44656c69766572794e6f74654d4453656e6465724f6e654c6f636174696f6e416e644973416374697665052464343563356265342d373765322d346131662d623666662d386430396364346263386261 AND key >= 1583576698492 AND key <= 1599128698492 LIMIT 5000 (see tombstone_warn_threshold)"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val logEntry = LogEntry.fromString(example, regex)
        logEntry.host = "testNode"
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+live +tombstone", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val tombstones = TombstoneWarnings()
        val response = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued - seems strange
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("2020-09-03|1")
        assertThat(response).contains("testNode|1")
        assertThat(response).contains("maf.timeseries_index|1|97.99")
    }

    @Test
    fun testDSE68SingleWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val example = "WARN  [CoreThread-15] 2022-05-18 18:04:08,680  NoSpamLogger.java:98 - Scanned over 1689 tombstone rows for query SELECT * FROM test_ks.test_tbl WHERE token(id) > -8660790630412484902 AND token(id) <= -8621134171302509557  - more than the warning threshold 1000"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val logEntry = LogEntry.fromString(example, regex)
        logEntry.host = "testNode"
        entries.add(logEntry)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.8.19", true)
        every { cluster.isDse } returns true
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Scanned +tombstone", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val tombstones = TombstoneWarnings()
        val response = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued since number of warn per day is less than 100
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("2022-05-18|1")
        assertThat(response).contains("testNode|1")
        assertThat(response).contains("test_ks.test_tbl|1|-")
    }

    @Test
    fun testMultipleWarning() {

        val entries: MutableList<LogEntry> = mutableListOf()
        val entry1 = "WARN  [SharedPool-Worker-1] 2020-09-03 10:25:03,316 ReadCommand.java:520 - Read 61 live rows and 2968 tombstone cells for query SELECT * FROM maf.timeseries_index WHERE systemid, bucket, partition = 30001, 159846, 0702052a44656c69766572794e6f74654d4453656e6465724f6e654c6f636174696f6e416e644973416374697665052464343563356265342d373765322d346131662d623666662d386430396364346263386261 AND key >= 1583576698492 AND key <= 1599128698492 LIMIT 5000 (see tombstone_warn_threshold)"
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val logEntry1 = LogEntry.fromString(entry1, regex)
        logEntry1.host = "testNode"
        entries.add(logEntry1)
        val entry2 = "WARN  [SharedPool-Worker-1] 2020-09-04 10:25:03,316 ReadCommand.java:520 - Read 561 live rows and 2968 tombstone cells for query SELECT * FROM maf.timeseries_index WHERE systemid, bucket, partition = 30001, 159846, 0702052a44656c69766572794e6f74654d4453656e6465724f6e654c6f636174696f6e416e644973416374697665052464343563356265342d373765322d346131662d623666662d386430396364346263386261 AND key >= 1583576698492 AND key <= 1599128698492 LIMIT 5000 (see tombstone_warn_threshold)"
        val logEntry2 = LogEntry.fromString(entry2, regex)
        logEntry2.host = "testNode"
        entries.add(logEntry2)

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.latest311()
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+live +tombstone", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val tombstones = TombstoneWarnings()
        val response = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued - seems strange
        assertThat(recs.size).isEqualTo(0)
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("2020-09-03|1")
        assertThat(response).contains("2020-09-04|1")
        assertThat(response).contains("testNode|2")
        assertThat(response).contains("maf.timeseries_index|2|91.04")

    }

    @Test
    fun testDSE68MultipleWarning() {
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val entries = arrayOf(
            "WARN  [CoreThread-15] 2022-05-18 18:04:08,680  NoSpamLogger.java:98 - Scanned over 1689 tombstone rows for query SELECT * FROM test_ks.tbl1 WHERE token(id) > -8660790630412484902 AND token(id) <= -8621134171302509557  - more than the warning threshold 1000",
            "WARN  [CoreThread-15] 2022-05-18 22:12:01,481  NoSpamLogger.java:98 - Scanned over 2112 tombstone rows for query SELECT * FROM test_ks.tbl1 WHERE token(id) >= token(27208732) AND token(id) <= 6432830243470897067 LIMIT 2147477647  - more than the warning threshold 1000",
            "WARN  [CoreThread-15] 2022-05-19 08:14:02,320  NoSpamLogger.java:98 - Scanned over 1023 tombstone rows for query SELECT * FROM test_ks.tbl2 WHERE token(id) > 8366352141232066227 AND token(id) <= 8406253750395140548  - more than the warning threshold 1000"
        ).map {
            val entry = LogEntry.fromString(it, regex)
            entry.host = "testNode"
            entry
        }

        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("6.8.19", true)
        every { cluster.isDse } returns true
        val searcher = mockk<Searcher>(relaxed = true)
        every { searcher.search("+Scanned +tombstone", LogLevel.WARN, 1000000) } returns entries

        val recs: MutableList<Recommendation> = mutableListOf()
        val tombstones = TombstoneWarnings()
        val response = tombstones.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // No Warning is issued since number of warn per day is less than 100
        assertThat(recs.size).isEqualTo(0)
        assertThat(response).contains("2022-05-18|2")
        assertThat(response).contains("2022-05-19|1")
        assertThat(response).contains("testNode|3")
        assertThat(response).contains("test_ks.tbl1|2|-")
        assertThat(response).contains("test_ks.tbl2|1|-")
    }
}