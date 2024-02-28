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

package com.datastax.montecristo.model.versions.cassandra

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logMessageParsers.TombstoneWarningMessage
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.versions.DatabaseVersion

abstract class Cassandra(val versionString: String) : DatabaseVersion {

    override fun supportsReadRepair(): Boolean {
        return true
    }

    override fun supportsVNodes(): Boolean {
        return true
    }

    override fun recommendedVNodeCount(): Int {
        return 16
    }

    override fun isSeda(): Boolean {
        return true
    }

    override fun isSafeToUseUDT(): Boolean {
        return true
    }

    override fun shouldDisableDebugLogging(): Boolean {
        return false
    }

    override fun showCassandra4Upgrade(): Boolean {
        return true
    }

    override fun isCommunityMaintained(): Boolean {
        return false
    }

    override fun supportsOffHeapMemtables(): Boolean {
        return true
    }

    override fun supportsThrift(): Boolean {
        return true
    }

    override fun maxPartitionSizeMetricName(): String {
        return "MaxPartitionSize"
    }

    override fun meanPartitionSizeMetricName(): String {
        return "MeanPartitionSize"
    }

    override fun estimatedRowCountMetricName(): String {
        return "EstimatedPartitionCount"
    }

    override fun lcsDefaultSSTableSize(): String {
        return "160"
    }

    override fun lcsDefaultFanOutSize(): String {
        return "10"
    }

    override fun searchLogForLargePartitionWarnings(searcher : Searcher, queryLimit : Int) : List<LogEntry> {
        return searcher.search("+large +partition", LogLevel.WARN, queryLimit)
    }

    override fun showChunkLengthKBNote(): Boolean {
        return false
    }

    override fun defaultPermissionsValidity(): String {
        return "2000"
    }

    override fun supportsCredentialValiditySetting(): Boolean {
        return false
    }

    override fun supportsIncrementalRepair(): Boolean {
        return false
    }
    override fun toString(): String {
        return versionString
    }

    override fun releaseMajorMinor() : String {
        return if (versionString.count{ c -> c == '.'} ==2) {
            versionString.substringBeforeLast(".")
        } else {
            versionString
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Cassandra) return false

        if (versionString != other.versionString) return false

        return true
    }

    override fun hashCode(): Int {
        return versionString.hashCode()
    }

    override fun parseLargePartitionSizeMessage(messages: List<String>): List<Pair<String, Long>> {
        val pattern = Regex("partition ([\\w]+[\\/]+[\\w.]+:[\\w:\\-\\/_]+) [\\(]+([0-9\\.]+)([MGTP]iB)[\\)]+")
        val parsedLogs = messages.mapNotNull { line -> pattern.find(line)?.groupValues }
        return parsedLogs.mapNotNull { groupValues ->
            if (groupValues.size >= 3 ) {
                Pair(
                    groupValues[1],
                    ByteCountHelper.parseHumanReadableByteCountToLong(groupValues[2] + groupValues[3])
                )
            } else {
                null
            }
        }
    }
    override fun searchLogForTombstones(searcher: Searcher, queryLimit: Int): List<TombstoneWarningMessage> {
        return searcher.search("+live +tombstone", LogLevel.WARN, queryLimit)
            .mapNotNull { TombstoneWarningMessage.fromLogEntry(it) }
    }

    override fun searchLogForBatches(searcher: Searcher, queryLimit: Int): List<LogEntry> {
        return searcher.search ("+BatchStatement.java", LogLevel.WARN, queryLimit)
    }

    fun parseOldLargePartitionSizeMessage(messages: List<String>, regex : String): List<Pair<String, Long>> {
        val pattern = Regex(regex)
        val parsedLogs = messages.mapNotNull { line -> pattern.find(line)?.groupValues }
        return parsedLogs.filter { groupValues -> groupValues.size >= 2 } .map { groupValues ->
            Pair(
                groupValues[1],
                groupValues[2].toLong())
        }
    }
}