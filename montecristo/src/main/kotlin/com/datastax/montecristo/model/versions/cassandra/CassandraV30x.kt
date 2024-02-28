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

import com.datastax.montecristo.model.versions.DatabaseVersion

class CassandraV30x  (versionIdentifier: String) : Cassandra(versionIdentifier)  {
    override fun supportsOffHeapMemtables(): Boolean {
        return false
    }
    override fun isSafeToUseUDT(): Boolean {
        return false
    }
    override fun isCommunityMaintained(): Boolean {
        return true
    }
    override fun latestRelease(): DatabaseVersion {
        return DatabaseVersion.latest30()
    }
    override fun recommendedOSSettingsLink(): String {
        return  "https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/install/installRecommendSettings.html"
    }
    override fun parseLargePartitionSizeMessage(messages: List<String>): List<Pair<String, Long>> {
        return super.parseOldLargePartitionSizeMessage(messages, "partition ([\\w]+[\\/]+[\\w.]+:[\\w:\\-_]+) [\\(]+([0-9]+) bytes[\\)]+")
    }
}