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

package com.datastax.montecristo.sections.configuration

import com.google.common.collect.Maps
import com.datastax.montecristo.fileLoaders.parsers.application.CassandraYamlParser
import com.datastax.montecristo.helpers.ConfigHelper
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import org.slf4j.LoggerFactory
import java.net.URL


class CustomSettings : DocumentSection {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val md = MarkdownTable("Setting", "Base", "Current")
                .orMessage("No customized setting was found")

        try {
            val baseConfig = CassandraYamlParser.parse(downloadBaseConfiguration(cluster.isDse, cluster.databaseVersion)).asMap()
            // This is using the first node as the arbitrarily chosen reference
            val sampleConfig = cluster.nodes.first().cassandraYaml.asMap()
            val customizedSettings = isolateCustomSettings(
                    removePasswordFields(baseConfig),
                    removePasswordFields(sampleConfig)
            )

            customizedSettings.entries.sortedBy { it.key }.forEach {
                md.addRow().addField(it.key).addField(it.value.first ?: "").addField(it.value.second ?: "")
            }
        } catch (e: RuntimeException) {
            logger.error("Failed looking up for custom settings in Cassandra configuration", e)
        }

        // since the data directories is by default a single value, if multiple values have been specified, it will become visible in this
        // section since the change of config to multiple will appear - so we check here and recommend here where it is visible, it is both an infrastructure change
        // as well as a config change, but we will file it under Infra which is the first part of the change, get the disk space in place ready.

        // multiple values are exposed as a csv on the return value instead of a single value, so if we detect a comma (split by it) we know we have
        // multiple values. If any nodes have that, then we know we have at least 1 node with multiple data directories.
        if (cluster.nodes.any { it.cassandraYaml.get("data_file_directories", "", true).split(",").size > 1 }) {
            recs.near(RecommendationType.INFRASTRUCTURE, "We recommend that you provision sufficient space on a single volume so that only a single data file directory is configured. The use of multiple data directories has significant disadvantages.")
        }

        args["table"] = md.toString()
        return compileAndExecute("configuration/configuration_custom_settings.md", args)

    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        fun downloadBaseConfiguration(isDse : Boolean, cassandraDatabaseVersion: DatabaseVersion): String {

            return if (!isDse) {
                logger.info("Downloading configuration for Cassandra version $cassandraDatabaseVersion")
                URL("https://raw.githubusercontent.com/apache/cassandra/cassandra-${cassandraDatabaseVersion}/conf/cassandra.yaml").readText()
            } else {
                // DSE files have to come from the local resources
                val resFile = this::class.java.getResource("/dse.config/${cassandraDatabaseVersion.releaseMajorMinor()}/${cassandraDatabaseVersion}/cassandra.yaml")
                if (resFile != null) {
                    resFile.readText()
                } else {
                    this::class.java.getResource("/dse.config/${cassandraDatabaseVersion.releaseMajorMinor()}/${cassandraDatabaseVersion.releaseMajorMinor() + ".x" }/cassandra.yaml")?.readText() ?: ""
                }
            }
        }

        fun isolateCustomSettings(base: Map<String, Any>, custom: Map<String, Any>): Map<String, Pair<Any, Any>> {
            val customSettings = Maps.newHashMap<String, Pair<Any, Any>>()
            val difference = Maps.difference(
                    ConfigHelper.removeNodeAndClusterSpecificValues(base),
                    ConfigHelper.removeNodeAndClusterSpecificValues(custom)
            )

            // covers cases where a setting is present in both base and custom, but differs in values
            difference.entriesDiffering().entries.forEach {
                customSettings[it.key] = Pair(it.value.leftValue(), it.value.rightValue())
            }

            // covers cases where a setting is present in custom only
            difference.entriesOnlyOnRight().entries.forEach {
                customSettings[it.key] = Pair("blank", it.value)
            }

            // for completeness, the opposite, where the setting is in base, but not in custom
            // we do not want to do this because it detects all the things we removed in .removeNodeAndClusterSpecificValues

            return customSettings
        }

        fun removePasswordFields(cassandraConfig: Map<String, Any>): Map<String, Any> {
            return cassandraConfig.entries.associate { entry ->
                val newKey = entry.key
                var newValue = entry.value
                if (entry.value is Map<*, *>) {
                    newValue = (entry.value as Map<*, *>).filterKeys { !(it as String).contains("password") }
                }
                Pair(newKey, newValue)
            }
        }
    }
}


