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

import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.model.versions.cassandra.CassandraV22x

class DseV4(versionIdentifier: String) : CassandraV22x(versionIdentifier) {
    override fun latestRelease(): DatabaseVersion {
        return DatabaseVersion.latestDSE4()
    }

    override fun recommendedOSSettingsLink(): String {
        return "https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/config/configRecommendedSettings.html"
    }
}