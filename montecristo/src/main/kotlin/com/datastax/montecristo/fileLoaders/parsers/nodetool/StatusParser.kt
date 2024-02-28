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
import com.datastax.montecristo.helpers.*
import com.datastax.montecristo.model.nodetool.Status
import com.datastax.montecristo.model.nodetool.StatusMetaData

object StatusParser : IFileParser<Status> {

    override fun parse(data: List<String>): Status {

        val nodes: List<StatusMetaData>

        nodes = mutableListOf()
        var dcName = ""
        for (line in data) {
            if (line.startsWith("Datacenter")) {
                dcName = line.split(": ")[1]
            }

            if (line.split(" ")[0] in arrayOf("UN", "UL", "UJ", "UM", "DN", "DL", "DJ", "DM", "DS")) {
                val values = line.split(" ").filter { value -> value != "" }
                val status = values[0]
                val address = values[1]
                val loadInBytes: Long = ByteCountHelper.parseHumanReadableByteCountToLong("${values[2]} ${values[3]}")
                val tokens = values[4]
                val ownership = values[5]
                val hostId = values[6]
                val rack = values[7]

                val metaData = StatusMetaData(status, address, loadInBytes, tokens.toInt(), ownership, hostId, rack, dcName)
                nodes.add(metaData)
            }
        }
        return Status(nodes.toList(), data.joinToString("\n"))
    }
}
