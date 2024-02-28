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

package com.datastax.montecristo.fileLoaders.parsers.nodetool

import com.datastax.montecristo.fileLoaders.parsers.IFileParser
import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.model.nodetool.Info


object InfoParser : IFileParser<Info> {

    override fun parse(data: List<String>): Info {

        var infoDataCenter = "UNKNOWN"
        var infoRack = "UNKNOWN"
        var infoLoadInBytes : Long = -1
        var infoUpTimeInSeconds : Long? = null

        for (line in data) {
            val lineComponents = line.split(":")

            when (lineComponents[0].trim()) {
                "Data Center" -> infoDataCenter = lineComponents[1].trim()
                "Rack" -> infoRack = lineComponents[1].trim()
                "Load" -> infoLoadInBytes = ByteCountHelper.parseHumanReadableByteCountToLong(lineComponents[1].trim())
                "Uptime (seconds)" -> infoUpTimeInSeconds = lineComponents[1].trim().toLong()
            }
        }

        return Info(infoDataCenter, infoRack, infoLoadInBytes, infoUpTimeInSeconds, data.joinToString("\n"))
    }
}