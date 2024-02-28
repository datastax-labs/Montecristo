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

package com.datastax.montecristo.model.metrics

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.ByteCountHelperUnits
import com.datastax.montecristo.helpers.Utils
import java.util.*
import kotlin.math.pow


/**
 * metrics extention for a list of server,metric pairs
 * Very useful when we're reporting the range of something, like table latency
 */

class ServerMetricList(private var data: MutableList<Pair<String, Double>>) {

    fun isEmpty() :Boolean {
        return data.isEmpty()
    }

    fun add(item: Pair<String, Double>) {
        data.add(item)
    }

    fun max(): Double? {
        return data.map { it.second }.maxOrNull()
    }

    fun maxFormatted2DP() : String {
        return if (data.size > 0) {
            String.format("%.2f", data.map { it.second }.maxOrNull() ?: "")
        } else {
            ""
        }
    }
    /**
     * Returns the min value from the list of servers for whatever metric this has been associated with
     */
    fun min(): Double? {
        return data.map { it.second }.minOrNull()
    }

    fun average(): Double {
        return data.map { it.second }.average()
    }

    fun sum(): Double {
        return data.sumOf { it.second }
    }

    fun variance(): Double {
        val mean = average()
        return data.map { (it.second - mean).pow(2.0) }.average()
    }

    fun sumAsHumanReadable(): String {
        return Utils.humanReadableCount(sum().toLong())
    }

    fun averageAsHumanReadable(): String {
        return Utils.humanReadableCount(average().toLong())
    }

    fun averageBytesAsHumanReadable(): String {
        return ByteCountHelper.humanReadableByteCount(average(), ByteCountHelperUnits.SI, Locale.US)
    }

    fun getByNodeOrDefault(node: String, default : Double): Double {
        for (x in data) {
            if (x.first == node) return x.second
        }
        return default
    }

    fun getData() : Map<String, Double> {
        return data.toMap()
    }
}
