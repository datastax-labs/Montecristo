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

package com.datastax.montecristo.model.versions.dse

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logMessageParsers.DseTombstoneWarningMessage
import com.datastax.montecristo.logs.logMessageParsers.TombstoneWarningMessage
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.versions.DatabaseVersion

class DseV68X(versionIdentifier: String) : Dse6Base(versionIdentifier) {
    override fun supportsReadRepair(): Boolean {
        return false
    }
    override fun isCommunityMaintained(): Boolean {
        return true
    }

    override fun latestRelease(): DatabaseVersion {
        return DatabaseVersion.latestDSE68()
    }

    override fun recommendedOSSettingsLink(): String {
        return "https://docs.datastax.com/en/dse/6.8/dse-dev/datastax_enterprise/config/configRecommendedSettings.html"
    }

    override fun searchLogForTombstones(searcher: Searcher, queryLimit: Int): List<TombstoneWarningMessage> {
        return searcher.search("+Scanned +tombstone", LogLevel.WARN, queryLimit)
            .mapNotNull { DseTombstoneWarningMessage.fromLogEntry(it) }
    }

    override fun searchLogForBatches(searcher: Searcher, queryLimit: Int): List<LogEntry> {
        return searcher.search ("+Batch +for +\"is of size\" ", LogLevel.WARN, queryLimit)
    }
}