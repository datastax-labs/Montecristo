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

package com.datastax.montecristo.model

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.schema.Schema
import com.datastax.montecristo.model.versions.DatabaseVersion
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

data class Cluster(val nodes: List<Node>,
                   val isAws: Boolean,
                   val isDse: Boolean,
                   val databaseVersion: DatabaseVersion,
                   val schema: Schema,
                   val blockedTasks: BlockedTasks, // TODO - this shouldn't be a top level object, feels like it should be attached to the nodes or metrics
                   val metricServer: IMetricServer,
                   val loadErrors: MutableList<LoadError>) {

        // config helpers
    fun getSetting(configSetting: String, configSource: ConfigSource, default: String = "", isList: Boolean = false): ConfigurationSetting {
        val values = // If config setting is commented, it'll get the default value
            nodes.associate { node ->
                // If config setting is commented, it'll get the default value
                if (configSource == ConfigSource.CASS) {
                    val foundValue = node.cassandraYaml.get(configSetting, "not_set", isList)
                    if (foundValue == "not_set") {
                        Pair(node.hostname, ConfigValue(false, default, ""))
                    } else {
                        Pair(node.hostname, ConfigValue(true, default, foundValue))
                    }
                } else {
                    val foundValue = node.dseYaml.get(configSetting, "not_set", isList)
                    if (foundValue == "not_set") {
                        Pair(node.hostname, ConfigValue(false, default, ""))
                    } else {
                        Pair(node.hostname, ConfigValue(true, default, foundValue))
                    }
                }
            }

            return ConfigurationSetting(name = configSetting, values = values)
    }

    fun getNode(nodeName : String) : Node? {
        return nodes.firstOrNull { it.hostname == nodeName }
    }

    fun isMultiDC() : Boolean {
        // is there just a single DC name?
        return getDCNames().size > 1
    }

    fun getDCNames() : List<String> {
        return nodes.map { it.info.dataCenter }.toSet().toList()
    }

    fun getNodesFromDC(dc : String) : List<Node> {
        return nodes.filter { it.info.dataCenter == dc }
    }

    fun getLogDurationsInHours(limitInDays : Long): Map<String, Double> {

        val logDurations = this.metricServer.getLogDurations() // map of node to min / max
        val mapOfDurations: Map<String, Double> = logDurations.mapValues {
            val minDate = Utils.tryParseDate (it.value.first )
            val maxDate = Utils.tryParseDate (it.value.second )
            val duration = minDate.until(maxDate, ChronoUnit.MINUTES) / 60.0
            minOf(duration, (limitInDays.toDouble() * 24.0))
        }
        return mapOfDurations
    }

    fun getLogMinDatesToUse(daysToInclude : Long) : Map<String, LocalDateTime> {
        val logDurations = this.metricServer.getLogDurations() // map of node to min / max
        val mapOfLogMinDates = logDurations.map {
            val minDate = Utils.tryParseDate (it.value.first )
            val maxDate = Utils.tryParseDate (it.value.second )

            val maxDateMinusXDays = maxDate.minusDays(daysToInclude)
            val minDateToUse = if (minDate.isAfter(maxDateMinusXDays)) { minDate } else { maxDateMinusXDays }

            Pair(it.key,  minDateToUse)
        }.toMap()
        return mapOfLogMinDates
    }

    fun isLogEntryParsingTruncated(daysToInclude : Long) : Map<String, Boolean> {
        val logDurations = this.metricServer.getLogDurations() // map of node to min / max
        val mapOfNodesTruncated = logDurations.map {
            val minDate = Utils.tryParseDate (it.value.first )
            val maxDate = Utils.tryParseDate (it.value.second )
            val maxDateMinus90Days = maxDate.minusDays(daysToInclude)
            Pair(it.key,  !minDate.isAfter(maxDateMinus90Days))
        }.toMap()
        return mapOfNodesTruncated
    }
}