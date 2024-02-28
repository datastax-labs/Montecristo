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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation

class AddressBindings : DocumentSection {

    private val customParts = StringBuilder()

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val listenAddress = cluster.getSetting("listen_address", ConfigSource.CASS)
        val listenInterface = cluster.getSetting("listen_interface", ConfigSource.CASS)
        val rpcAddress = cluster.getSetting("rpc_address", ConfigSource.CASS)
        val rpcInterface = cluster.getSetting("rpc_interface", ConfigSource.CASS)
        val broadcastAddress = cluster.getSetting("broadcast_address", ConfigSource.CASS)
        val broadcastInterface = cluster.getSetting("broadcast_interface", ConfigSource.CASS)
        val broadcastRpcAddress = cluster.getSetting("broadcast_rpc_address", ConfigSource.CASS)
        val broadcastRpcInterface = cluster.getSetting("broadcast_rpc_interface", ConfigSource.CASS)
        var customSettingsFound = 0

        customParts.append("The following settings are configured for address bindings : \n\n")
        customParts.append("```\n")

        if (listenAddress.values.isNotEmpty()) {
            customParts.append("listen_address: ${listenAddress.getSingleValue()} \n")
            ++customSettingsFound
        }

        if (listenInterface.values.isNotEmpty()) {
            customParts.append("listen_interface: ${listenInterface.getSingleValue()} \n\n")
            ++customSettingsFound
        }

        if (broadcastAddress.values.isNotEmpty()) {
            customParts.append("broadcast_address: ${broadcastAddress.getSingleValue()} \n")
            ++customSettingsFound
        }

        if (broadcastInterface.values.isNotEmpty()) {
            customParts.append("broadcast_interface: ${broadcastInterface.getSingleValue()} \n\n")
            ++customSettingsFound
        }

        if (rpcAddress.values.isNotEmpty()) {
            customParts.append("rpc_address: ${rpcAddress.getSingleValue()} \n")
            ++customSettingsFound
        }

        if (rpcInterface.values.isNotEmpty()) {
            customParts.append("rpc_interface: ${rpcInterface.getSingleValue()} \n\n")
            ++customSettingsFound
        }

        if (broadcastRpcAddress.values.isNotEmpty()) {
            customParts.append("broadcast_rpc_address: ${broadcastRpcAddress.getSingleValue()} \n")
            ++customSettingsFound
        }

        if (broadcastRpcInterface.values.isNotEmpty()) {
            customParts.append("broadcast_rpc_interface: ${broadcastRpcInterface.getSingleValue()} \n")
            ++customSettingsFound
        }

        customParts.append("```\n\n")

        if (customSettingsFound > 0) {
            args["address-bindings"] = customParts.toString()
        } else {
            args["address-bindings"] = "No address bindings found"
        }
        return compileAndExecute("configuration/configuration_address_bindings.md", args)
    }
}
