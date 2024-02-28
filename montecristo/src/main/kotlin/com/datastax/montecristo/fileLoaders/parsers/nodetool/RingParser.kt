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
import com.datastax.montecristo.model.nodetool.Ring
import com.datastax.montecristo.model.nodetool.Tokens
import java.math.BigInteger


object RingParser : IFileParser<List<Ring>> {


    override fun parse(data: List<String>): List<Ring> {
        val ringTokens = mutableListOf<Ring>()
        var dcName: String
        var startRange = BigInteger.ZERO
        for (line in data) {
            val trimLine = line.trim()
            // Is there a better way to do this? if a node was called To or Note: (unlikely) this fails.
            if (trimLine.split(" ")[0] !in arrayOf("Address", "==========", "Warning:", "To", "Note:", "nodetool") && trimLine.isNotEmpty()) {
                if (trimLine.startsWith("Datacenter")) {
                    dcName = trimLine.split(": ")[1]
                    ringTokens.add(Ring(datacenter = dcName))
                } else {
                    // Address and ========== / blank lines stripped out, either its a token for a node, or its the
                    // starting token from the end, in which case the line would just be 1 value of a numeric token.
                    if (trimLine.split(" ").size == 1) {
                        // This is the line for the roll-around token, to be used as the starting range for the next line.
                        startRange = trimLine.toBigInteger() + BigInteger.ONE
                    } else if (trimLine.startsWith("/") || trimLine.startsWith("SLF4J: ")) {
                        // ignore preceding SLF4J logging errors
                        // ignore any final line at the end of this file `/opt/cassandra/bin/nodetool rc=0` will cause a fatal error here
                        continue
                    } else {
                        val values = trimLine.split(" ").filter { value -> value != "" }
                        val nodeAddress = values[0]
                        val endRange = values[values.size - 1].toBigInteger()

                        val nodeTokens = Tokens(startRange = startRange, endRange = endRange)
                        // make sure the node address is present, if not add in an empty list
                        ringTokens[ringTokens.size - 1].nodes.putIfAbsent(nodeAddress, mutableListOf())
                        // put the node entry in for this nodeAddress, for the start / end range
                        // marked safe since we have ensured nodes.get(nodeAddress is not null)
                        ringTokens[ringTokens.size - 1].nodes[nodeAddress]?.add(nodeTokens)
                        // set up the start range for the next token as the end of the previous + 1
                        startRange = endRange + BigInteger.ONE
                    }
                }
            }
        }
        return ringTokens.toList()
    }
}

