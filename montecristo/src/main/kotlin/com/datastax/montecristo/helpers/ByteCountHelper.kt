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

package com.datastax.montecristo.helpers

import com.datastax.montecristo.model.application.ParsedByteCount
import java.math.BigDecimal
import java.util.*
import kotlin.math.ln
import kotlin.math.pow

fun Double.humanBytes(): String {
    return ByteCountHelper.humanReadableByteCount(this)
}

fun Long.humanBytes(): String {
    return ByteCountHelper.humanReadableByteCount(this)
}

enum class ByteCountHelperUnits(val base: Double, val powToStr: Array<String>) {
    SI(1000.0, arrayOf("B", "kB", "MB", "GB", "TB", "PB", "EB")),
    BINARY(1024.0, arrayOf("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"))
}

object ByteCountHelper {
    fun humanReadableByteCount(bytes: Double, units: ByteCountHelperUnits = ByteCountHelperUnits.SI, locale: Locale = Locale.ENGLISH): String {
        return humanReadableByteCount(bytes.toLong(), units, locale)
    }

    fun humanReadableByteCount(bytes: Long, units: ByteCountHelperUnits = ByteCountHelperUnits.SI, locale: Locale = Locale.ENGLISH): String {
        // When using the smallest unit no decimal point is needed, because it's the exact number.
        if (bytes < units.base) {
            return bytes.toString() + " " + units.powToStr[0]
        }
        val exponent = (ln(bytes.toDouble()) / ln(units.base)).toInt()
        val unit = units.powToStr[exponent]
        return "${(bytes / units.base.pow(exponent.toDouble())).round(locale)} $unit"
    }

    fun parseHumanReadableByteCountToLong(textSize: String): Long {
        // Power and base units have been taken from this reference: https://en.wikipedia.org/wiki/Binary_prefix
        var returnValue: Long = -1
        val parserPattern = Regex("([\\d. ]+)([EPTGMKk])(i?B)")
        if (parserPattern.matches(textSize)) {

            val number = parserPattern.find(textSize)!!.groups[1]!!.value.toDouble()
            val prefix = parserPattern.find(textSize)!!.groups[2]!!.value
            val binaryMarker = parserPattern.find(textSize)!!.groups[3]!!.value
            val pow = convertPrefix(prefix)
            val base: Long = convertBinaryMarker(binaryMarker)
            var bytes = BigDecimal(number)
            bytes = bytes.multiply(BigDecimal.valueOf(base).pow(pow))
            returnValue = bytes.toLong()
        }
        return returnValue
    }

    fun parseByteCountWithRate(rateString: String): ParsedByteCount {
        // Parses strings like "MB/s", "MiB/s", "GiB/s", "GB/s" and returns the numeric value and unit without the rate suffix
        // Pattern: number + unit (with optional prefix and i for binary) + optional /s or /sec
        val parserPattern = Regex("([\\d. ]+)\\s*([EPTGMKk])(i?B)(?:/s)?(?:/sec)?", RegexOption.IGNORE_CASE)
        val match = parserPattern.find(rateString) ?: return ParsedByteCount(-1, "Invalid format")

        val number = match.groups[1]!!.value.toDouble()
        val prefix = match.groups[2]!!.value
        val binaryMarker = match.groups[3]!!.value

        val pow = convertPrefix(prefix)
        val base: Long = convertBinaryMarker(binaryMarker)
        var bytes = BigDecimal(number)
        bytes = bytes.multiply(BigDecimal.valueOf(base).pow(pow))

        // Reconstruct the unit without the /s suffix
        val unit = prefix + binaryMarker

        return ParsedByteCount(bytes.toLong(), unit)
    }

    private fun convertBinaryMarker(binaryMarker: String): Long {
        val base: Long = when (binaryMarker) {
            "B" -> 1000     // SI (decimal) units
            "iB" -> 1024    // Binary units
            else -> 0
        }
        return base
    }

    private fun convertPrefix(prefix: String): Int {
        val pow = when (prefix) {
            "E" -> 6
            "P" -> 5
            "T" -> 4
            "G" -> 3
            "M" -> 2
            "k", "K" -> 1
            else -> 0
        }
        return pow
    }
}