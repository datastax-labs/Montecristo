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

package com.datastax.montecristo.logs.logMessageParsers

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.model.os.GCAlgorithm
import java.time.LocalDateTime

//Container class for GC pause information
data class GCPauseMessage(val algorithm: GCAlgorithm,
                          val timeInMS: Int,
                          val date: LocalDateTime,
                          val host: String) {

    companion object {
        val regex = "([\\w]*) GC in ([0-9]+)ms".toRegex()
        private val g1regex = "(\\d+) ms".toRegex()
        private val cms20 = "GC for ConcurrentMarkSweep: (\\d+) ms".toRegex()
        private val parNew20 = "GC for ParNew: (\\d+) ms".toRegex()


        // WTF???
        private val parallelMarkSweep = "GC for PS MarkSweep: (\\d+) ms".toRegex()


        fun fromLogEntry(entry: LogEntry): GCPauseMessage? {
            val message = entry.message?:""
            val result = regex.find(message)

            when {
                result != null -> {
                    val algorithm = when (result.groupValues[1]) {
                        "ParNew" -> GCAlgorithm.PARNEW
                        "ConcurrentMarkSweep" -> GCAlgorithm.CMS
                        else -> GCAlgorithm.UNKNOWN
                    }
                    val time = result.groupValues[2]
                    return GCPauseMessage(algorithm, time.toInt(), entry.getDate(), entry.host!!)
                }
                message.contains("G1") -> {
                    val parsed = g1regex.find(message)
                    val time = parsed!!.groupValues[1]
                    return GCPauseMessage(GCAlgorithm.G1GC, time.toInt(), entry.getDate(), host = entry.host!!)
                }
                message.contains("GC for ConcurrentMarkSweep") -> { // 2.0 CMS format, i think
                    val parsed = cms20.find(message)
                    return GCPauseMessage(GCAlgorithm.CMS, parsed!!.groupValues[1].toInt(), entry.getDate(), entry.host!!)
                }
                message.contains(" GC for ParNew") -> {
                    val parsed = parNew20.find(message)
                    return GCPauseMessage(GCAlgorithm.PARNEW, parsed!!.groupValues[1].toInt(), entry.getDate(), entry.host!!)
                }
                message.contains("PS MarkSweep") -> {
                    val parsed = parallelMarkSweep.find(message)
                    return GCPauseMessage(GCAlgorithm.PARNEW, parsed!!.groupValues[1].toInt(), entry.getDate(), entry.host!!)
                }
                else -> return null
            }
        }
    }
}