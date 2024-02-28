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
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.model.nodetool.GossipInfo

object GossipInfoParser : IFileParser<Map<String, GossipInfo>> {

    override fun parse(data: List<String>): Map<String, GossipInfo> {
        // ok, the gossip info is an entry per node in the cluster - later on we are going to be interested in 'this' node, but knowing what each
        // of the nodes thing about each other could be interesting long term, so lets get it all parsed and loaded.
        // drop the first because the split will split the first entry to a blank, followed by the IP.
        val infoSections: List<String> = data.joinToString("\n").split("/").drop(1)

        return if (infoSections.isNotEmpty()) // get back to the lines, they are in a defined order
            infoSections.associate {
                // get back to the lines, they are in a defined order

                val section = it.split("\n")
                val ipAddress = section[0]
                val generation: Long = getSettingValue(section, "generation", 1).toLongOrNull() ?: 0
                val heartbeat: Long = getSettingValue(section, "heartbeat", 1).toLongOrNull() ?: 0
                val status: String = getSettingValue(section, "STATUS", 2).split(",")[0]
                val schema: String = getSettingValue(section, "SCHEMA", 2)
                val dc: String = getSettingValue(section, "DC", 2)
                val rack: String = getSettingValue(section, "RACK", 2)
                val releaseVersion: String = getSettingValue(section, "RELEASE_VERSION", 2)
                val internalIp: String = getSettingValue(section, "INTERNAL_IP", 2)
                val rpcAddress: String = getSettingValue(section, "RPC_ADDRESS", 2)
                val dseOptions = if (dseSettingsExist(section)) {
                    val dseOptionsJson = getDseJsonSettingValue(section)
                    Utils.parseJson(dseOptionsJson)
                } else {
                    Utils.parseJson("{}")
                }
                val hostId: String = getSettingValue(section, "HOST_ID", 2)
                val rpcReady: Boolean = getSettingValue(section, "RPC_READY", 2).toBoolean()
                val gossipInfo = GossipInfo(
                    generation,
                    heartbeat,
                    status,
                    schema,
                    dc,
                    rack,
                    releaseVersion,
                    internalIp,
                    rpcAddress,
                    dseOptions,
                    hostId,
                    rpcReady
                )
                Pair(ipAddress, gossipInfo)
            } else mapOf()
    }

    private fun getSettingValue(data: List<String>, setting: String, subSection: Int): String {
        val values = data.filter { it.trim().startsWith(setting) }
        return if (values.isNotEmpty()) {
            val splitData = values.first().split(":")
            if (splitData.size >= subSection) {
                splitData[subSection]
            } else {
                // the section being requested does not exist.
                ""
            }
        } else {
            ""
        }
    }

    private fun dseSettingsExist(data: List<String>): Boolean {
        return data.any { it.trim().startsWith("X_11_PADDING") || it.trim().startsWith("DSE_GOSSIP_STATE") }
    }

    private fun getDseJsonSettingValue(data: List<String>): String {
        val values = data.filter { it.trim().startsWith("X_11_PADDING") || it.trim().startsWith("DSE_GOSSIP_STATE") }
        return if (values.isNotEmpty()) {
            val splitData = values.first().substringAfter("{")
            if (splitData.isNotEmpty()) {
                "{$splitData" // the split stripped off the starting brace
            } else {
                // the section being requested does not exist.
                "{}"
            }
        } else {
            "{}"
        }
    }
}

