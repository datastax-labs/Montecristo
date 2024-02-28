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

package com.datastax.montecristo.sections.security

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.RecommendationType

class NetworkEncryption : DocumentSection {

    private val customParts = StringBuilder()

    /**
    Check if encryption is enabled for node to node and client to node communications.
    Recommend enabling them if they're not.
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        var networkEncryptionSettingsFound = 0

        val serverToServerEncryption = cluster.getSetting("server_encryption_options.internode_encryption", ConfigSource.CASS, "none")

        if (serverToServerEncryption.values.isNotEmpty()) {
            ++networkEncryptionSettingsFound

            val internodeEncryption = serverToServerEncryption.getSingleValue()

            if (serverToServerEncryption.isConsistent()) {
                customParts.append("Server encryption is currently ${if (internodeEncryption == "none") "disabled" else "enabled"} : \n\n")
                customParts.append("```\n")
                customParts.append("server_encryption_options: \n")
                customParts.append("    internode_encryption: $internodeEncryption\n")
                customParts.append("```\n\n")
            } else {
                customParts.append("Server encryption is inconsistent across the cluster\n\n")
                customParts.append("```\n")
                val groupedByServerToServerEncryption = serverToServerEncryption.values.entries.groupBy { entry -> entry.value.getConfigValue() }
                groupedByServerToServerEncryption.forEach { value ->
                    run {
                        customParts.append("internode_encryption=${value.key} : ${value.value.size} node${if (value.value.size > 1) "s" else ""}\n")
                    }
                }
                serverToServerEncryption.values.forEach {
                    customParts.append(it.key + " = " + it.value.getConfigValue() + "\n")
                }
                customParts.append("```\n\n")
            }

            if (isDisabledSomewhere(serverToServerEncryption) || internodeEncryption == "none") {
                recs.add(Recommendation(RecommendationPriority.LONG, RecommendationType.SECURITY, serverEncryptionRecommendation))
            }
        }

        val clientToServerEncryption = cluster.getSetting("client_encryption_options.enabled", ConfigSource.CASS, "false")

        if (clientToServerEncryption.values.isNotEmpty()) {
            ++networkEncryptionSettingsFound

            val clientEncryptionEnabled = clientToServerEncryption.getSingleValue().toBoolean()

            if (clientToServerEncryption.isConsistent()) {
                customParts.append("Client encryption is currently ${if (!clientEncryptionEnabled) "disabled" else "enabled"} : \n\n")
                customParts.append("```\n")
                customParts.append("client_encryption_options: \n")
                customParts.append("    enabled: $clientEncryptionEnabled\n")
                customParts.append("```\n\n")
            } else {
                customParts.append("Client encryption is inconsistent across the cluster\n\n")
                customParts.append("```\n")
                val groupedByClientToServerEncryption = clientToServerEncryption.values.entries.groupBy { entry -> entry.value.getConfigValue() }
                groupedByClientToServerEncryption.forEach { value ->
                    run {
                        customParts.append("enabled=${value.key} : ${value.value.size} node${if (value.value.size > 1) "s" else ""}\n")
                    }
                }
                customParts.append("\n")

                clientToServerEncryption.values.forEach {
                    customParts.append("${it.key} = ${it.value.getConfigValue()}\n")
                }
                customParts.append("```\n\n")
            }

            if (isDisabledSomewhere(clientToServerEncryption) || !clientEncryptionEnabled) {
                recs.add(Recommendation(RecommendationPriority.LONG, RecommendationType.SECURITY, clientEncryptionRecommendation))
            }
        }

        if (networkEncryptionSettingsFound > 0) {
            args["encryption"] =  customParts.toString()
        } else {
            args["encryption"] = "No network encryption settings found"
        }

        return compileAndExecute("security/security_network_encryption.md", args)
    }

    private val serverEncryptionRecommendation = "We recommend implementing server-to-server encryption for all communications to enforce security. Further work will need to be done to document the process in a runbook as nodes using encryption and nodes with it disabled are unable to see one another."
    private val clientEncryptionRecommendation = "We recommend implementing client-to-server encryption to enforce security. Do note that passwords to connect to Cassandra are sent in plain text when encryption is turned off."

    fun isDisabledSomewhere(encryptionSetting: ConfigurationSetting): Boolean {
        val encryptionSettingMaps = (encryptionSetting.getDistinctValues())
        return encryptionSettingMaps.any {
            !it.isSet || it.value == "false" || it.value == "disabled" || it.value == "none"
        }
    }

}
