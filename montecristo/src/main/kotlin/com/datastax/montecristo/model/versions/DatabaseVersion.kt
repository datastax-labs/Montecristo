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

package com.datastax.montecristo.model.versions

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logMessageParsers.TombstoneWarningMessage
import com.datastax.montecristo.model.versions.cassandra.*
import com.datastax.montecristo.model.versions.dse.*
import java.net.URL

interface DatabaseVersion {

    fun defaultPermissionsValidity() : String

    fun recommendedVNodeCount() : Int

    fun isSeda() : Boolean
    fun isSafeToUseUDT() : Boolean
    fun isCommunityMaintained() : Boolean
    fun showChunkLengthKBNote() : Boolean

    fun shouldDisableDebugLogging() : Boolean
    fun latestRelease() : DatabaseVersion
    fun releaseMajorMinor() : String

    fun maxPartitionSizeMetricName() : String
    fun meanPartitionSizeMetricName() : String
    fun estimatedRowCountMetricName() : String
    fun lcsDefaultSSTableSize() : String
    fun lcsDefaultFanOutSize() : String

    fun searchLogForTombstones(searcher : Searcher, queryLimit : Int) : List<TombstoneWarningMessage>
    fun parseLargePartitionSizeMessage(message : List<String>) : List<Pair<String,Long>>

    fun searchLogForBatches(searcher: Searcher, queryLimit: Int) : List<LogEntry>

    fun recommendedOSSettingsLink() : String

    fun searchLogForLargePartitionWarnings(searcher : Searcher, queryLimit : Int) : List<LogEntry>

    fun showCassandra4Upgrade() : Boolean

    fun supportsReadRepair(): Boolean
    fun supportsVNodes() : Boolean
    fun supportsCredentialValiditySetting() : Boolean
    fun supportsOffHeapMemtables(): Boolean
    fun supportsThrift() : Boolean
    fun supportsIncrementalRepair() : Boolean // by supports we mean its ok to use

    companion object {
        fun fromString(versionIdentifier: String, isDse: Boolean = false ): DatabaseVersion {
            if (isDse)
            {
                return when {
                    versionIdentifier.startsWith("4.") -> DseV4(versionIdentifier)
                    versionIdentifier.startsWith("5.0") -> DseV5x(versionIdentifier)
                    versionIdentifier.startsWith("5.1") -> DseV51x(versionIdentifier)
                    versionIdentifier.startsWith("6.0") -> DseV6X(versionIdentifier)
                    versionIdentifier.startsWith("6.7") -> DseV67X(versionIdentifier)
                    versionIdentifier.startsWith("6.8") -> DseV68X(versionIdentifier)
                    else -> DseV68X(versionIdentifier)  // version is unknown, return latest
                }
            } else
            {
                return when {
                    versionIdentifier.startsWith("1.") -> CassandraV1x(versionIdentifier)
                    versionIdentifier.startsWith("2.0") -> CassandraV20x(versionIdentifier)
                    versionIdentifier.startsWith("2.1") -> CassandraV21x(versionIdentifier)
                    versionIdentifier.startsWith("2.2") -> CassandraV22x(versionIdentifier)
                    versionIdentifier.startsWith("3.11") -> CassandraV311x(versionIdentifier)
                    versionIdentifier.startsWith("3.0") -> CassandraV30x(versionIdentifier)
                    versionIdentifier.startsWith("3.") -> CassandraV3x(versionIdentifier)
                    versionIdentifier.startsWith("4.0") -> CassandraV40x(versionIdentifier)
                    versionIdentifier.startsWith("4.1") -> CassandraV41x(versionIdentifier)
                    else -> CassandraV311x(versionIdentifier)  // version is unknown, return 3.11, which is the most common
                }
            }
        }


        fun latest12() : DatabaseVersion {
            return fromString("1.2.19")
        }

        fun latest20() : DatabaseVersion {
            return fromString("2.0.17")
        }

        fun latest21() : DatabaseVersion {
            return fromString("2.1.22")
        }

        fun latest22() : DatabaseVersion {
            return fromString("2.2.19")
        }

        fun latest30() : DatabaseVersion {
            return fromString("3.0.29")
        }

        fun latest311() : DatabaseVersion {
            return fromString("3.11.16")
        }

        fun latest40() : DatabaseVersion {
            return fromString("4.0.12")
        }

        fun latest41() : DatabaseVersion {
            return fromString("4.1.4")
        }

        fun latestDSE4() : DatabaseVersion {
            return fromString("4.8.16", true)
        }
        fun latestDSE50() : DatabaseVersion {
            return fromString("5.0.16", true)
        }
        fun latestDSE51() : DatabaseVersion {
            // Grab the release notes from github
            val releaseNoteLines = URL("https://raw.githubusercontent.com/datastax/release-notes/master/DSE_5.1_Release_Notes.md").readText().split("\n")
            val latestRelease = locateLatestRelease(releaseNoteLines, "# Release notes for 5.1.")
            return fromString(latestRelease, true)
        }
        fun latestDSE60() : DatabaseVersion {
            return fromString("6.0.19", true)
        }
        fun latestDSE67() : DatabaseVersion {
            return fromString("6.7.17", true)
        }
        fun latestDSE68() : DatabaseVersion {
            // Grab the release notes from github
            val releaseNoteLines = URL("https://raw.githubusercontent.com/datastax/release-notes/master/DSE_6.8_Release_Notes.md").readText().split("\n")
            val latestRelease = locateLatestRelease(releaseNoteLines, "# Release notes for 6.8.")
            return fromString(latestRelease, true)
        }

        internal fun locateLatestRelease(releaseNotes : List<String>, releaseNotePattern: String): String {
            // find the lines in the markdown which refer to releases of a version, then take the first (which is the top entry)
            val latestReleaseNoteLine = releaseNotes.filter { l -> l.startsWith(releaseNotePattern) }.first()
            // the release number is at the end of the line
            return latestReleaseNoteLine.substringAfterLast(" ")
        }
    }
}
