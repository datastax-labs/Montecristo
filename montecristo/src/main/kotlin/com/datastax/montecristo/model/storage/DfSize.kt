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

package com.datastax.montecristo.model.storage

data class DfSize  (val data : List<String>, val path : String) {

    fun usedSpacePercentage(volume : String) : Int? {
        val volumeRow = data.firstOrNull { it.startsWith("/dev/$volume") }
        if (!volumeRow.isNullOrEmpty ()) {
            val regex = "(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)".toRegex()
            if (volumeRow.matches(regex)) {
                val groups = regex.find(volumeRow)?.groups
                if (groups?.size ?: 0 >= 5) {
                    val percentageString = groups!![5]?.value ?: ""
                    return percentageString.replace("%", "").toIntOrNull()
                }
            }
        }
        return null
    }
}