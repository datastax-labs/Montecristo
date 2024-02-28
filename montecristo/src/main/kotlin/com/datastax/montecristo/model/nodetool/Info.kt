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

package com.datastax.montecristo.model.nodetool

data class Info (
        val dataCenter : String = "UNKNOWN",
        val rack : String = "UNKNOWN",
        val loadInBytes : Long = -1,
        val upTimeInSeconds : Long? = null,
        val rawContent: String = ""
) {
        fun getHostID(): String {
                val idRow = rawContent.split("\n").firstOrNull { it.trim().startsWith("ID") }
                return if (idRow.isNullOrEmpty()) {
                        ""
                } else {
                        if (idRow.contains (":")) {
                                idRow.split(":")[1].trim()
                        } else {
                                ""
                        }
                }
        }

        fun getUptimeInHours() : Double? {
                return if (upTimeInSeconds != null ) {
                        (upTimeInSeconds / 3600.0)
                } else {
                        null
                }
        }

        fun getUptimeInDays() : String? {
                return if (upTimeInSeconds != null ) {
                        String.format("%.2f",(upTimeInSeconds / 86400.0 ))
                } else {
                        null
                }
        }
}