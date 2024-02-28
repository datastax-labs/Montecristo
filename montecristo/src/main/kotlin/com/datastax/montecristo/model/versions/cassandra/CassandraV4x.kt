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

abstract class CassandraV4x (versionIdentifier: String) : Cassandra(versionIdentifier)  {
    override fun supportsReadRepair(): Boolean {
        return false
    }
    override fun supportsThrift(): Boolean {
        return false
    }
    override fun isCommunityMaintained(): Boolean {
        return true
    }

    override fun supportsCredentialValiditySetting(): Boolean {
        return true
    }

    override fun recommendedOSSettingsLink(): String {
        return "https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/install/installRecommendSettings.html"
    }

    override fun supportsIncrementalRepair(): Boolean {
        return true
    }
}
