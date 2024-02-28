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

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.ByteCountHelperUnits
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.Workload
import com.datastax.montecristo.model.os.InstanceMeta
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.long
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import java.util.*

class InfrastructureOverview : DocumentSection {

    private val vnodesRecommendationPre30 =  """
        We recommend changing the number of vnodes to 64. The current value in use will make operations like repair and bootstrap last longer.
        Further work will need to be done to document the process in a runbook as the process is non-trivial.
    """.trimIndent().replace('\n', ' ')
    private val vnodesRecommendationPost30 = """
        We recommend changing the number of vnodes to 16. The current value in use will make operations like repair and bootstrap last longer.
        This change requires bootstrap nodes in a new datacenter using the new token allocation algorithm by setting the `allocate_tokens_for_keyspace` value
        appropriately in `cassandra.yaml`. Further work will need to be done to document the process in a runbook as the process is non-trivial.
    """.trimIndent().replace('\n', ' ')


    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        try {
            // Main Infrastructure table
            val infraOverview = MarkdownTable("Hostname", "Listen Address", "OS", "DC", "Rack", "Workload", "CPU", "RAM", "Load", "Tokens")
            val nodesGroupedByDC = cluster.nodes.groupBy {  it.info.dataCenter }.toList()
            nodesGroupedByDC.forEachIndexed { index, dcToNodeList ->
                dcToNodeList.second.sortedBy { it.info.dataCenter + "-" + it.info.rack + " - " + it.listenAddress }
                    .forEach { node ->
                        infraOverview.addRow()
                            .addField(node.hostname)
                            .addField(node.listenAddress)
                            .addField(node.osConfiguration.osReleaseName)
                            .addField(node.info.dataCenter)
                            .addField(node.info.rack)
                            .addField(node.workloads)
                            .addField(node.osConfiguration.lsCpu.getCpuThreads())
                            .addField(
                                ByteCountHelper.humanReadableByteCount(
                                    ByteCountHelper.parseHumanReadableByteCountToLong(
                                        "${node.osConfiguration.memInfo.memTotal} kB"
                                    ),
                                    ByteCountHelperUnits.BINARY
                                )
                            )
                            .addField(ByteCountHelper.humanReadableByteCount(node.info.loadInBytes))
                            .addField(node.cassandraYaml.vNodes)
                    }
                    // blank row for the end of the DC, as long as its not the last
                    if (index != nodesGroupedByDC.size -1) {
                        infraOverview.addRow().addBlankRow(10)
                    }
            }
            args["infra"] = infraOverview.toString()

            // AWS Section of the output
            val servers = cluster.metricServer.getServers()
            var instanceTypes = mapOf<String, Int>()
            args["showAWS"] = true
            args["awsInstances"] = if (cluster.isAws) {
                val serversByRegion = servers.groupingBy { Pair(it.aws_region, it.aws_instance_type) }.eachCount()
                val instancesTable = MarkdownTable("Instance Type", "Region", "Count", "vCores", "Memory", "EBS Optimized")
                val meta = InstanceMeta()
                serversByRegion.forEach { (t, count) ->
                    instancesTable.addRow()
                            .addField(t.second)
                            .addField(t.first)
                            .addField(count)
                            .addField(meta.getInstance(t.second)?.vCPU ?: 0)
                            .addField(meta.getInstance(t.second)?.memory ?: 0)
                            .addField(meta.getInstance(t.second)?.ebsOptimized.toString())
                }
                instanceTypes = servers.groupingBy { it.aws_instance_type }.eachCount()
                instancesTable.toString()
            } else {
                args["showAWS"] = false
                ""
            }
            args["clusterName"] = cluster.getSetting("cluster_name", ConfigSource.CASS).getSingleValue()

            // Recommendation Checks
            checkForMixedWorkloadDCs(cluster, recs)
            checkRackCount( cluster, recs)
            checkIfDSEFSEnabledIncorrectly(cluster, recs)
            checkIfDCsHaveAtLeast3Nodes(cluster, recs)
            checkIfNodesHaveMismatchedHardware(cluster, recs)
            checkIfNodesUsingMinimumRecommendedHardware(cluster, recs, args)
            checkIfAWSInstancesAreDifferentTypes(args, instanceTypes, recs)
            checkVNodesValue(cluster, recs)
            checkIfTooMuchDataPerNode(cluster, recs)

        } catch (e: Exception) {
            cluster.loadErrors.add(LoadError("All","Error generating infrastructure/infrastructure_overview.md $e"))
        }
        return compileAndExecute("infrastructure/infrastructure_overview.md", args)
    }

    internal fun checkVNodesValue(cluster: Cluster, recs: MutableList<Recommendation>) {
        // if there are no vNodes settings anywhere, then it is on single token architecture
        val highestTokenCount = cluster.nodes.map { node -> node.cassandraYaml.vNodes.toString().toInt() }.maxOrNull() ?: 1

        if (cluster.databaseVersion.supportsVNodes()) {
            if (cluster.databaseVersion.recommendedVNodeCount() == 16 && highestTokenCount > cluster.databaseVersion.recommendedVNodeCount() ) {
                recs.long(RecommendationType.INFRASTRUCTURE, vnodesRecommendationPost30)
            } else if (cluster.databaseVersion.recommendedVNodeCount() == 64 && highestTokenCount > cluster.databaseVersion.recommendedVNodeCount()) {
                recs.long(RecommendationType.INFRASTRUCTURE, vnodesRecommendationPre30)
            }
        }
    }

    private fun checkIfAWSInstancesAreDifferentTypes(args: MutableMap<String, Any>, instanceTypes: Map<String, Int>, recs: MutableList<Recommendation>) {
        if (args["showAWS"] == true) {
            if (instanceTypes.keys.size > 1) {
                recs.near(
                    RecommendationType.INFRASTRUCTURE,
                    "Your cluster is using different AWS instance types across nodes. We recommend using a single instance class/size to have a consistent performance throughout the cluster."
                )
            }
        }
    }

    internal fun checkIfNodesUsingMinimumRecommendedHardware(cluster: Cluster, recs: MutableList<Recommendation>, args: MutableMap<String, Any>) {
        // due to the cpu count not being guaranteed to be discovered, in the event of all CPU counts being null,
        // we use the value of 0
        val minCpus = Collections.min(cluster.nodes.map { it.osConfiguration.lsCpu.getCpuThreads() })
        if (minCpus in 1..7) {
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "We recommend running ${args["software"]} on hosts with at least 8 CPU cores. ${args["software"]} uses a highly concurrent architecture with a large amount of parallelism. The hosts currently in use have only $minCpus CPU cores which is suboptimal for this architecture."
            )
        }
        if (minCpus > 32 && !cluster.isDse) {
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "We recommend running Cassandra on hosts with at most 32 cores. Cassandra uses a highly concurrent, staged event-driven architecture (SEDA) with a large amount of parallelism. As the core count increases the benefit per core diminishes."
            )
        }
        if (cluster.nodes.any { it.osConfiguration.memInfo.memTotal ?: 0 < 32_000_000 }) {
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "We recommend running ${args["software"]} on hosts with at least 32 GB of RAM, generally 64 GB or higher is a preferable in production."
            )
        }
    }

    internal fun checkIfNodesHaveMismatchedHardware(cluster: Cluster, recs: MutableList<Recommendation>) {
        // Does a DC have different cpu / memory values - different values in different DCs are considered ok, since they could be handling very different workloads
        // such as OLTP vs Spark, or its a DR site / witness site.
        val dcsWithMismatchedHardware = mutableListOf<String>()
        for (dc in cluster.getDCNames()) {
            val nodesFromDC = cluster.getNodesFromDC(dc)
            if ((nodesFromDC.map { it.osConfiguration.lsCpu.getCpuThreads() }.toSet().size > 1)) {
                dcsWithMismatchedHardware.add(dc)
            } else {
                // memory - we wish to tolerate minor differences, up to 4 GB of tolerance.
                // so we can't toSet the values, but if we look at max vs min, we know
                // these values are in kb, so its a difference of 4 GB expressed as KB, e.g. 4 million k.
                if ((nodesFromDC.maxOf {
                        it.osConfiguration.memInfo.memTotal ?: 0
                    } - nodesFromDC.minOf { it.osConfiguration.memInfo.memTotal ?: 0 }) > 4_000_000) {
                    dcsWithMismatchedHardware.add(dc)
                }
            }
        }
        if (dcsWithMismatchedHardware.size > 0) {
            val dcList = dcsWithMismatchedHardware.joinToString(", ")
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "Your cluster is using different hardware profiles (cpu / memory) within a single data center for the following data centers : $dcList. We recommend using the same cpu / memory within a DC to have a consistent performance throughout the cluster."
            )
        }
    }

    internal fun checkIfDCsHaveAtLeast3Nodes(cluster: Cluster, recs: MutableList<Recommendation>) {
        // check if a DC has less than 3 nodes - there are situations where they might be intended, but it should be called out to be discarded
        // instead of ignored.
        val dcWithLessThan3Nodes =
            cluster.nodes.groupingBy { it.info.dataCenter }.eachCount().filter { it.value < 3 }.map { it.key }
        if (dcWithLessThan3Nodes.isNotEmpty()) {
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "We recommend that each DC has a minimum of 3 nodes, the following DC(s) have less than 3 : ${
                    dcWithLessThan3Nodes.joinToString(", ")
                }."
            )
        }
    }

    internal fun checkForMixedWorkloadDCs(cluster: Cluster, recs: MutableList<Recommendation>) {
        // check if the workload type per DC is identical
        // construct a unique list of pairs DC / Workload
        val setOfDCWorkloads = cluster.nodes.map { Pair(it.info.dataCenter, it.workloads) }.toSet()
        // get a grouped count and filter to those with more than 1 workload type
        val dcWorkloadCount = setOfDCWorkloads.groupingBy { it.first }.eachCount().filter { it.value > 1 }

        if (dcWorkloadCount.isNotEmpty()) {
            recs.near(
                RecommendationType.INFRASTRUCTURE,
                "We recommend that you do not run inconsistent workload types within a single DC, the following DCs have mixed workload types: " + dcWorkloadCount.keys.joinToString(
                    ","
                )
            )
        }
    }

    internal fun checkRackCount(cluster : Cluster, recs: MutableList<Recommendation>) {
        val listOfDCsWithOddRackConfig = mutableListOf<String>()
        // get the number of racks per DC
        val racksPerDC = cluster.nodes.map { Pair(it.info.dataCenter, it.info.rack) }.toSet().groupingBy { it.first }.eachCount()

        racksPerDC.filter { dcEntry -> dcEntry.value != 1 &&  dcEntry.value != 3}.forEach {
            listOfDCsWithOddRackConfig.add(it.key)

        }
        if (listOfDCsWithOddRackConfig.isNotEmpty()) {
            recs.long(RecommendationType.INFRASTRUCTURE, "We recommend that you use either 1 or 3 racks within each DC. The following DC(s) are using alternative rack layouts: ${listOfDCsWithOddRackConfig.joinToString(", ")}. Racks may not be altered after a node has bootstrapped, changing the rack layout requires careful planning, please reach out to DataStax services if you need help with this activity.")
        }
    }

    internal fun checkIfDSEFSEnabledIncorrectly(cluster: Cluster, recs: MutableList<Recommendation>) {
        // If the cluster is DSE, and has no nodes running Analytics, but DSEFS is enabled on any nodes, then DSEFS should be switched off.
        if (cluster.isDse
            && cluster.nodes.none { it.workload.contains(Workload.ANALYTICS) }
        ) {
            val nodesWithDSEFS =
                cluster.nodes.filter { it.dseYaml.get("dsefs_options.enabled", "false", false) == "true" }
            if (nodesWithDSEFS.isNotEmpty()) {
                recs.near(
                    RecommendationType.INFRASTRUCTURE,
                    "DSE FS is enabled on ${nodesWithDSEFS.size} node(s), but there is no corresponding analytics workload. We recommend DSE FS is disabled on all nodes."
                )
            }
        }
    }

    internal fun checkIfTooMuchDataPerNode(cluster: Cluster, recs: MutableList<Recommendation>) {
        val nodesWithMoreThan2TBData = cluster.nodes.filter { it.info.loadInBytes > 2_000_000_000_000 }
        if (nodesWithMoreThan2TBData.isNotEmpty()) {
            recs.near(
                    RecommendationType.INFRASTRUCTURE,
                    "${nodesWithMoreThan2TBData.size} nodes are storing more than 2TB of data each. We recommend that the number of nodes within the cluster is expanded to reduce the amount stored per node."
            )
        }
    }
}