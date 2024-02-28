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

package com.datastax.montecristo.fileLoaders.parsers.os

import com.datastax.montecristo.fileLoaders.parsers.IFileParser
import com.datastax.montecristo.model.storage.LsBlk

object LsBlkParser : IFileParser<Map<String,LsBlk>> {

    override fun parse(data: List<String>): Map<String,LsBlk> {

        // need to remove spaces and and lines
        val trimmedData = data.map { it.trim() }
        val resultsMap = mutableMapOf<String, LsBlk>()
        for (processedLine in trimmedData) {
            val regex = "[└─|├─|─]*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*\\/.*\\s(\\d)\$".toRegex()
            if (processedLine.matches(regex)) {
                val groups = regex.find(processedLine)?.groups
                if (!groups.isNullOrEmpty() && groups.size != 4) {
                    // device name is [1]
                    // Rotational is [6]
                    // format is at [2]
                    // some of the rows are devices and not the mount points.
                    // the mount points all are hung under a physical device
                    val deviceName = groups[1]?.value ?: "unknown"
                    val format = groups[3]?.value ?: "unknown"
                    val rotational = groups[4]?.value == "1"
                    resultsMap[deviceName] = LsBlk(format, rotational)
                }
            }
        }
        return resultsMap.toMap()
    }
}