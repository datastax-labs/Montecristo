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

package com.datastax.montecristo.fileLoaders.parsers.application

import com.datastax.montecristo.model.application.JvmSettings
import com.datastax.montecristo.model.os.GCAlgorithm

object JVMSettingsParser {

    fun parse(cassandraCommandLine: String?): JvmSettings {
        if (cassandraCommandLine.isNullOrBlank())
            return JvmSettings(GCAlgorithm.UNKNOWN, 0, 0, emptyList())

        val heapSizeRegex = Regex("-Xmx([0-9]+[mMgG]{1})")
        val newGenSizeRegex = Regex("-Xmn([0-9]+[mMgG]{1})")
        val newGenG1SizeRegex = Regex("-XX:NewSize=([0-9]+[mMgG]{1})")
        val gcFlagsRegex = Regex("(-XX:[\\S\\w]*)|(-Xloggc:[\\S\\w]*)")
        val usesCMS = cassandraCommandLine.contains("UseParNewGC")
        val usesG1 = cassandraCommandLine.contains("-XX:+UseG1GC")
        val heapSize: Long = if (heapSizeRegex.find(cassandraCommandLine) != null) parseHeapSize(heapSizeRegex.find(cassandraCommandLine)!!.groups[1]!!.value) else 1
        var newGenSize: Long? = null
        newGenSize = if (usesCMS) {
            parseHeapSize(newGenSizeRegex.find(cassandraCommandLine)!!.groups[1]!!.value)
        } else {
            // newsize might not be specified
            val matchResult = newGenG1SizeRegex.find(cassandraCommandLine)

            if (matchResult != null && matchResult.groups.isNotEmpty())
                parseHeapSize(matchResult.groups[1]!!.value)
            else {
                0
            }
        }
        val gcFlags: MutableSet<String> = mutableSetOf()
        gcFlagsRegex.findAll(cassandraCommandLine).forEach {
            gcFlags.add(it.value)
        }

        val gcAlgorithm =
            when {
                usesCMS -> GCAlgorithm.CMS
                usesG1 -> GCAlgorithm.G1GC
                else -> GCAlgorithm.UNKNOWN
            }

        return JvmSettings(gcAlgorithm = gcAlgorithm, heapSize = heapSize, newGenSize = newGenSize, gcFlags = gcFlags.sorted())
    }

    fun parseHeapSize(textHeapSize: String): Long {
        return when {
            textHeapSize.contains("m", ignoreCase = true) -> {
                // Megabytes
                textHeapSize.replace(Regex("[mM]"), "").toLong() * 1024 * 1024
            }
            textHeapSize.contains("g", ignoreCase = true) -> {
                // Gigabytes
                textHeapSize.replace(Regex("[gG]"), "").toLong() * 1024 * 1024 * 1024
            }
            else -> {
                0
            }
        }
    }
}