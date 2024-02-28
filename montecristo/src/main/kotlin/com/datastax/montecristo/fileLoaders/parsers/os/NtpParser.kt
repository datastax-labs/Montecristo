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
import com.datastax.montecristo.model.os.Ntp

object NtpParser : IFileParser<Ntp> {

    override fun parse(data: List<String>): Ntp {

        val regex = "^\\*[^ ]+".toRegex()
        if (data.isNotEmpty()) {
            data.forEach {
                if (regex.containsMatchIn(it)) {
                    val serverName = it.split(" ").getOrNull(0) ?: "UNKNOWN_SERVER"
                    return parseOffsetAndJitter(serverName, it)
                }
            }
        }
        // if we get here, we had no data, or we could not parse what we had
        return Ntp("UNKNOWN_SERVER", null, null, null)
    }


    private fun parseOffsetAndJitter(serverName: String, line: String?): Ntp {
        val r = Regex("([0-9]+)\\s+[0-9]+\\s+-?[0-9]+\\.[0-9]+\\s+(-?[0-9]+\\.[0-9]+)\\s+(-?[0-9]+\\.[0-9]+)$")
        val groups = r.find(line ?: "")?.groups
        val poll = groups?.get(1)?.value?.toInt()
        val offset = groups?.get(2)?.value?.toDouble()
        val jitter = groups?.get(3)?.value?.toDouble()
        return Ntp(serverName, poll, offset, jitter)
    }
}
