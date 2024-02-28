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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.utils.HumanCount
import org.apache.cassandra.cql3.CQLStatement
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.commons.lang3.reflect.FieldUtils
import java.text.DecimalFormat
import java.text.NumberFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*
import kotlin.math.ln
import kotlin.math.pow


fun Double.round(locale: Locale = Locale.ENGLISH): Double {
    return Utils.round(this, locale)
}

fun Long.toHumanCount(): HumanCount {
    return HumanCount(this)
}

// make this stop breaking shit
fun Double.toPercent(defaultOnNull: String? = null): String {
    return try {
        """${(this * 100).round()}%"""
    } catch (e: Exception) {
        defaultOnNull ?: throw e
    }
}

fun String.stripHTMLComments(): String {
    return this.replace(Regex("<!--.*?-->"), "").trim()
}

object Utils {
    private const val DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

    fun tryParseDate(value : String) : LocalDateTime {
        return try {
            val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
            if (value.isNotBlank()) {
                LocalDateTime.parse(value, formatter)
            } else {
                LocalDateTime.now()
            }
        } catch (ex : DateTimeParseException) {
            LocalDateTime.now()
        }
    }

    // converts a number a something like 8.1 k, 4.2 M (million), 6.2 B (billion)
    // useful for request counts
    fun humanReadableCount(count: Long, locale: Locale = Locale.ENGLISH): String {
        if (count < 1000)
            return count.toString()

        var exp = (ln(count.toDouble()) / ln(1000.0)).toInt()

        val suffix = listOf("thousand", "million", "billion", "trillion")
        if (exp > suffix.size)
            exp = suffix.size
        val num = count / 1000.0.pow(exp.toDouble())

        return String.format(locale, "%.1f %c", num, "kMBT"[exp - 1])
    }

    fun parseJson(jsonString: String): JsonNode {
        val mapper = ObjectMapper()
        mapper.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
        return mapper.readTree(jsonString)
    }

    fun round(num: Double, locale: Locale = Locale.ENGLISH): Double {
        try {
            val decimalFormat: DecimalFormat = (NumberFormat.getNumberInstance(locale) as DecimalFormat)
            decimalFormat.applyPattern("##.##")
            return decimalFormat.format(num).toDouble()
        } catch (e: Exception) {
            println("Could not parse $num???")
            throw e
        }
    }


    /*
     * Displays each value for a given setting from the cassandra.yaml file, with the number of nodes that use it.
     * Also displays the list of node with its value for that setting.
     * To be used when creating markdown files for the Cassandra config section, when the config element !().isConsistent()
     */
    fun displayInconsistentConfig(entries: Set<Map.Entry<String, ConfigValue>>): String {
        val groupedByValue = entries.groupBy { entry -> entry.value.getConfigValue() }

        val customParts = StringBuilder()
        groupedByValue.forEach { value ->
                customParts.append("${colonToEquals(value.key)} : ${value.value.size} node${if (value.value.size > 1) "s" else ""}\n")
        }
        customParts.append("  \n")
        //        entries.forEach({ entry -> customParts.append(entry.key + " = " + entry.value + "\n") })
        entries.forEach { entry -> customParts.append(entry.key + " = " + colonToEquals(entry.value.getConfigValue()) + "\n") }

        return customParts.toString()
    }


    /*
     * Formats the standard output for Cassandra yaml settings.
     * Handles displaying the appropriate message if the setting is inconsistent throughout the cluster.
     */
    fun formatCassandraYamlSetting(configurationSetting: ConfigurationSetting): String {
        val output = StringBuilder()
        if (configurationSetting.isConsistent()) {
            output.append("`${configurationSetting.name}` is configured as follows: \n\n")
            output.append("```\n")
            output.append("${configurationSetting.name}: ${configurationSetting.getSingleValue()} \n")
            output.append("```\n\n")
        } else {
            output.append("`${configurationSetting.name}` setting is inconsistent across the cluster: \n\n")
            output.append("```\n")
            output.append(displayInconsistentConfig(configurationSetting.values.entries))
            output.append("```\n\n")
        }

        return output.toString()
    }

    fun parseCQL(string: String): CQLStatement.Raw? {
        return QueryProcessor.parseStatement(convertThriftEmptyStrings(stripCustomJavaTypes(string)))
    }

    fun readFieldForce(obj: Any, name: String): Any? {
        val f = FieldUtils.getField(obj.javaClass, name, true)
        if (f == null) {
            print("Why null?")
        }
        return FieldUtils.readField(f, obj, true)
    }

    fun stripCustomJavaTypes(cql : String) : String {
        // custom java class paths as data types cause a problem, primarily because the class they reference does not exist on our class path
        // so the parser is unable to get the details of the type.
        // For the purposes of Montecristo analysis, we can consider them binary objects (which is not far from reality), so we convert java custom types
        // to the type of blob, before parsing.

        // The table will have entries like this in the field list portion
        // geopointCoordinate 'org.apache.cassandra.db.marshal.PointType',
        // the difficulty is in finding and replacing them.

        // there is no guarantee that is is in the org.apache.cassandra namespace, it could be totally custom namespace, as long as they have
        // extended the abstract type.

        // Rules for finding them.
        // They only occur on CREATE TABLE statements and CREATE TYPE statements
        // they occur between the CREATE TABLE opening and closing parens (which is hard to figure out as well.) - but we can't accurately locate this
        // closing parens, the primary key could be in line. However, the WITH statement is a marker we can use to split where we process.
        // For CREATE TYPE statements, we just process the whole statement.

        return when {
            cql.trim().startsWith("CREATE TABLE") -> {
                val splitCql = cql.split("WITH")
                // first part must be processed
                val firstPart = splitCql[0]
                // second part can be left alone - its the table settings which we want to avoid processing
                val secondPart = "WITH" + splitCql[1]
                val processedFirstPart = firstPart.replace("(\\'.*\\')".toRegex() ,"blob")
                processedFirstPart + secondPart
            }
            cql.trim().startsWith("CREATE TYPE") -> {
                cql.replace("(\\'.*\\')".toRegex() ,"blob")
            }
            else -> {
                cql
            }
        }
    }

    private fun convertThriftEmptyStrings(cql : String) : String {
        // In CREATE TABLE, Thrift-as-CQL can introduce a column definition similar to:
        //    "" map<blob, blob>,
        // And the CQL parser cannot handle it.
        return if (cql.trim().startsWith("CREATE TABLE") && cql.contains("WITH COMPACT STORAGE")) {
            cql.replace("\"\" map","THRIFT_MAP map")
        } else {
            cql
        }
    }

    /**
     * Applies common cleanup to the code
     * We're going to put the size tag around the code block, then the backticks
     * Remove rc=0 at the end
     */
    fun code(s: String): String {
        return "{{% size 9 %}}\n```\n" + s.trim().removeSuffix("nodetool rc=0").trim() + "\n```\n{{% /size }}"
    }

    private fun colonToEquals(value: Any): String {
        return value.toString().replace(":", "=")
    }
}

