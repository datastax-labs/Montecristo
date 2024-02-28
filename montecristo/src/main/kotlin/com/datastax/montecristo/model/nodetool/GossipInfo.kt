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

package com.datastax.montecristo.model.nodetool

import com.fasterxml.jackson.databind.JsonNode
import com.datastax.montecristo.model.Workload

data class GossipInfo(val generation: Long, val heartbeat: Long, val status: String, val schema: String, val dc: String, val rack: String, val releaseVersion: String, val internalIp: String, val rpcAddress: String, val dseOptions: JsonNode, val hostId: String, val rpcReady: Boolean) {

    fun getWorkloads(): List<Workload> {
        val workloads = mutableListOf<Workload>()
        // add in the default base
        workloads.add(Workload.CASSANDRA)
        // check search / spark
        if (dseOptions.has("workloads") ) {
            val dseWorkload = dseOptions.get("workloads").toString()
            if (dseWorkload.contains("Analytics")) {
                workloads.add(Workload.ANALYTICS)
            }
            if (dseWorkload.contains("Search")) {
                workloads.add(Workload.SEARCH)
            }
        }
        // graph is held in a different setting in the json
        if (dseOptions.has("graph")) {
            val graphSetting = dseOptions.path("graph")
            if (graphSetting.toString() == "true") {
                workloads.add(Workload.GRAPH)
            }
        }
        return workloads.toList()
    }
}