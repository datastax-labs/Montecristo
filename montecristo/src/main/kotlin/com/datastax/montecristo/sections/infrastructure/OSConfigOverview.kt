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
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class OSConfigOverview : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val resultTable = MarkdownTable("Hostname", "Setting", "Current Value")
        args["recommendedSettingsLink"] = cluster.databaseVersion.recommendedOSSettingsLink()
        val clusterSize = cluster.nodes.size

        // We need to know which is the user running C* / DSE
        // ps-aux is as good as we have but it might not be reliable in finding the user name due to longer names being cut shorter.
        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.limits.getMemLock(it.osConfiguration.psAux.getCassandraRunningUser())) }
            .filter { it.second!= "unlimited" },
            "memlock", "Max locked-in-memory", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.limits.getNoFile(it.osConfiguration.psAux.getCassandraRunningUser())) }
            .filter { it.second.toIntOrNull() ?: 0 < 100000 },
            "nofile", "Open File Descriptors Limit", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.limits.getNProc(it.osConfiguration.psAux.getCassandraRunningUser())) }
            .filter { it.second.toIntOrNull() ?: 0 < 32768 },
            "nproc", "Number of Processes", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.limits.getAddressSetting(it.osConfiguration.psAux.getCassandraRunningUser())) }
            .filter { it.second != "unlimited" },
            "as", "Address Space Limit ", resultTable, clusterSize
        )

        // max_map_count
        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.vmMaxMapCount ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 < 1048575 },
            "vm.max_map_count", "Maximum Memory Map Count", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.netRmemMax ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 != 16777216L },
            "net.core.rmem_max", "Networking : Maximum Receive Window Size", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.netWmemMax ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 != 16777216L },
            "net.core.wmem_max", "Networking : Maximum Send Window Size", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.netRmemDefault ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 != 16777216L },
            "net.core.rmem_default", "Networking : Default Receive Window Size", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.netWmemDefault ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 != 16777216L },
            "net.core.wmem_default", "Networking : Default Send Window Size", resultTable, clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.sysctl.netOptMemMax ?: "") }
            .filter { it.second.toLongOrNull() ?: 0 != 40960L },
            "net.core.optmem_max", "Networking : Memory Allocation", resultTable, clusterSize
        )

        // tcp.ipv4 - done slightly different because the string value is a bit of a fuzzy match
        val rmemRegex = "4096\\W*87380\\W*16777216".toRegex()
        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.sysctl.netIpv4TcpRmem ?: "") }
            .filter { !it.second.matches(rmemRegex) },
            "net.ipv4.tcp_rmem", "Networking : ipv4 min, default and maximum receive buffer", resultTable,clusterSize
        )
        val wmemRegex = "4096\\W*65536\\W*16777216".toRegex()
        outputResults(cluster.nodes.map { Pair(it.hostname,  it.osConfiguration.sysctl.netIpv4TcpWmem ?: "") }
            .filter { !it.second.matches(wmemRegex) },
            "net.ipv4.tcp_wmem", "Networking : ipv4 min, default and maximum send buffer", resultTable,clusterSize
        )

        outputResults(cluster.nodes.map { Pair(it.hostname, it.osConfiguration.hugePages.isSetNever().toString() ) }
            .filter { it.second != "true" },
            "never", "Transparent Huge Page Defrag", resultTable, clusterSize
        )
        args["resultTable"] = resultTable.orMessage("The settings checked all match the recommended production settings.")

        val issues = resultTable.rows.size
        if (issues > 0) {
            recs.near (RecommendationType.INFRASTRUCTURE, "We recommend reviewing the operating system configurations against the deployment guides. There were $issues configuration issues found.")
        }
        return compileAndExecute("infrastructure/infrastructure_osconfig_overview.md", args)
    }

    private fun outputResults(
        nodes: List<Pair<String, String>>,
        setting: String,
        description: String,
        resultTable: MarkdownTable,
        clusterSize: Int
    ) {
        if (nodes.size == clusterSize) {
            resultTable.addRow()
                .addField("All Nodes")
                .addField("$description ($setting)")
                .addField(nodes.first().second)
        } else {
            nodes.forEach {
                resultTable.addRow()
                    .addField(it.first)
                    .addField("$description ($setting)")
                    .addField(it.second)
            }
        }
    }
}