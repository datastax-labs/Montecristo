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

package com.datastax.montecristo.model.os

data class PsAux(val data : List<String>, val path : String) {

    fun getNodeProcessLine() : String? {
        return data.firstOrNull{ it.contains("CassandraDaemon") }
                ?: data.firstOrNull{ it.contains("DseModule") }
                ?: data.firstOrNull{ it.contains("/usr/share/cassandra") ||
                    it.contains("/usr/local/cassandra") ||
                    it.contains("/usr/share/dse/cassandra") ||
                    it.contains("/opt/dse/") }
    }

    fun getCassandraRunningUser() : String {
        val runningUser = getNodeProcessLine()?.split(" ")?.first() ?: ""

        return if (runningUser == "cassand+") {
            "cassandra"
        } else {
            runningUser
        }
    }
}