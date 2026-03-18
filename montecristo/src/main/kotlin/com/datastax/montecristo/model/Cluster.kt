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

import com.datastax.montecristo.helpers.ByteCountHelper
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

    private data class SelectedConfigValue(val configSetting: String, val value: String, val isLegacy: Boolean, val defaultValue: String, val defaultUnits: String)

    // config helpers
    fun getSetting(configSetting: String, configSource: ConfigSource, default: String = "", isList: Boolean = false): ConfigurationSetting {
        val values = // If config setting is commented, it'll get the default value
            nodes.associate { node ->
                // If config setting is commented, it'll get the default value
                val yaml = if (configSource == ConfigSource.CASS) node.cassandraYaml else node.dseYaml
                val foundValue = yaml.get(configSetting, "not_set", isList)
                if (foundValue == "not_set") {
                    Pair(node.hostname, ConfigValue( false, default,"","", configSetting))
                } else {
                    Pair(node.hostname, ConfigValue (true, default, foundValue, "", configSetting,))
                }
            }

            return ConfigurationSetting(name = configSetting, values = values)
    }

    fun getSetting(configSource: ConfigSource,
                   configSetting: String,
                   defaultValue: String = "",
                   defaultUnits: String="",
                   legacyConfigSetting: String,
                   legacyDefaultValue: String = "",
                   legacyDefaultUnits: String="",
                   isList: Boolean = false): ConfigurationSetting {
        val values = // If config setting is commented, it'll get the default value
            nodes.associate { node ->
                val yaml = if (configSource == ConfigSource.CASS) node.cassandraYaml else node.dseYaml
                val currentValue = yaml.get(configSetting, "not_set", isList)
                val legacyValue = yaml.get(legacyConfigSetting, "not_set", isList)
                val currentConfigValue = SelectedConfigValue(configSetting, currentValue, isLegacy = false, defaultValue, defaultUnits)
                val legacyConfigValue = SelectedConfigValue(legacyConfigSetting, legacyValue, isLegacy = true, legacyDefaultValue, legacyDefaultUnits)

                val selectedConfig = selectSettingValue(yaml, currentConfigValue,legacyConfigValue, this.databaseVersion.hasUnitYamlValues())
                if (selectedConfig == null) {
                    Pair(node.hostname, ConfigValue(false, legacyDefaultValue, "", legacyDefaultUnits, configSetting))
                } else if (legacyDefaultUnits.isNotEmpty()) {
                    val valueToParse = if (selectedConfig.isLegacy) {
                        // Add units to legacy values so they can be parsed like modern unit-bearing values.
                        "${selectedConfig.value} $legacyDefaultUnits"
                    } else {
                        selectedConfig.value
                    }
                    parseSetting(selectedConfig.configSetting, valueToParse, node.hostname, legacyDefaultValue, legacyDefaultUnits)
                } else {
                    Pair(node.hostname, ConfigValue( true, legacyDefaultValue, selectedConfig.value, legacyDefaultUnits, selectedConfig.configSetting))
                }
            }
        // could be a mix of config names used here, but in the report we are only going to mention the first one.
        return ConfigurationSetting(name = values.firstNotNullOf{c -> c.value.configSetting}, values = values)
    }

    private fun selectSettingValue(
        yaml: com.datastax.montecristo.model.application.YamlConfig,
        currentConfigValue: SelectedConfigValue,
        legacyConfigValue: SelectedConfigValue,
        supportsUnitValues : Boolean
    ): SelectedConfigValue? {
        val hasCurrent = currentConfigValue.value != "not_set"
        val hasLegacy = legacyConfigValue.value != "not_set"

        return when {
            !hasCurrent && !hasLegacy -> null
            !supportsUnitValues -> legacyConfigValue
            hasCurrent && !hasLegacy -> currentConfigValue
            !hasCurrent && hasLegacy -> legacyConfigValue
            else -> {
                val currentLineNumber = yaml.lineFor(currentConfigValue.configSetting)
                val legacyLineNumber = yaml.lineFor(legacyConfigValue.configSetting)
                if (currentLineNumber == null || legacyLineNumber == null || currentLineNumber >= legacyLineNumber) {
                    currentConfigValue
                } else {
                    legacyConfigValue
                }
            }
        }
    }

    private fun parseSetting(
        configSetting: String,
        valueFound: String,
        hostName: String,
        defaultValue: String,
        defaultUnits: String,
    ): Pair<String, ConfigValue> {
        // are we expecting this setting to have units? (and does the DB even support them? ignore if it doesn't
        if (defaultUnits != "") {
            val parsedConfig = ByteCountHelper.parseByteCountWithRate(valueFound)
            return Pair(hostName, ConfigValue( true, defaultValue, parsedConfig.bytes.toString(), parsedConfig.unit, configSetting))
        } else {
            return Pair(hostName, ConfigValue( true, defaultValue, valueFound, defaultUnits, configSetting))
        }
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