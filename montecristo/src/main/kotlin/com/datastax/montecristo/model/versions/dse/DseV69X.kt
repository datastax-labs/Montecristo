package com.datastax.montecristo.model.versions.dse

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logMessageParsers.DseTombstoneWarningMessage
import com.datastax.montecristo.logs.logMessageParsers.TombstoneWarningMessage
import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.versions.DatabaseVersion

class DseV69X(versionIdentifier: String) : Dse6Base(versionIdentifier) {
    override fun supportsReadRepair(): Boolean {
        return false
    }
    override fun isSupported(): Boolean {
        return true
    }

    override fun latestRelease(): DatabaseVersion {
        return DatabaseVersion.latestDSE69()
    }

    override fun recommendedOSSettingsLink(): String {
        return "https://docs.datastax.com/en/dse/6.9/dse-dev/datastax_enterprise/config/configRecommendedSettings.html"
    }

    override fun searchLogForTombstones(searcher: Searcher, queryLimit: Int): List<TombstoneWarningMessage> {
        return searcher.search("+Scanned +tombstone", LogLevel.WARN, queryLimit)
            .mapNotNull { DseTombstoneWarningMessage.fromLogEntry(it) }
    }

    override fun searchLogForBatches(searcher: Searcher, queryLimit: Int): List<LogEntry> {
        return searcher.search ("+Batch +for +\"is of size\" ", LogLevel.WARN, queryLimit)
    }
}