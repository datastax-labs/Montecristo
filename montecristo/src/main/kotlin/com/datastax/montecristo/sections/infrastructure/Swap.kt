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

package com.datastax.montecristo.sections.infrastructure

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.*
import com.datastax.montecristo.utils.MarkdownTable

class Swap : DocumentSection {

    /**
     /os/meminfo:SwapCached     0 kB
     /os/meminfo:SwapFree       0 kB
     /os/meminfo:SwapTotal      0 kB

     /os/sysctl.txt: vm.swappiness = 60
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        // if there's swap reported via swapTotal note it
        // if swappiness is > 1, suggest it be lowered
        val swapTotal = cluster.nodes.maxOf { it.osConfiguration.memInfo.swapTotal ?: 0 }
        val vmswap = cluster.nodes.maxOf { it.osConfiguration.sysctl.vmswappiness?.toIntOrNull() ?: 0 }

        if(swapTotal > 0 && vmswap > 1) {
            recs.immediate(RecommendationType.INFRASTRUCTURE,"Swap is enabled and set to $vmswap, which can cause performance to dip when swap is used.  We recommend disabling swap by setting vm.swappiness = 1.")
        } else if (swapTotal > 0) {
            recs.long(RecommendationType.INFRASTRUCTURE,"Swap space is enabled and set to $swapTotal, with swappiness disabled.  We recommend removing the swap partition.")
        } else if(swapTotal == 0L && vmswap > 1) {
            recs.near(RecommendationType.INFRASTRUCTURE,"Swap space is set to zero but swappiness is set to $vmswap.  We recommend setting vm.swappiness to 1 to prevent machines from accidentally using swap in the case of a configuration mistake in which a swap partition is created.")
        }

        // we need to know if the settings are consistent, using a set we can then see if we need to display the report
        val setOfValues = cluster.nodes.map {
            Pair(it.osConfiguration.memInfo.swapTotal, it.osConfiguration.sysctl.vmswappiness)
        }.toSet()

        // we only show the swap report if we have different values
        val swapReport = if(setOfValues.size > 1) {
            val swapTable = MarkdownTable("Node", "Swap Partition Size", "vm.swappiness")
            cluster.nodes.forEach { node ->
                swapTable.addRow()
                        .addField(node.hostname)
                        .addField(node.osConfiguration.memInfo.swapTotal ?: 0)
                        .addField(node.osConfiguration.sysctl.vmswappiness ?: 0)

            }
            recs.immediate(RecommendationType.INFRASTRUCTURE,"We recommend using uniform system settings, with swap disabled on all nodes in a cluster.")
            swapTable.toString()
        } else {
            ""
        }

        args["swapReport"] = swapReport

        return compileAndExecute("infrastructure/infrastructure_swap.md", args)
    }
}