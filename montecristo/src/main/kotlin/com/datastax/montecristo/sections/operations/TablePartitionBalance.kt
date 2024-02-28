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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.toHumanCount
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MarkdownTable

class TablePartitionBalance : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // If we are happy at the data load, skip this section
        if (isDataLoadBalanced(cluster)) {
            return ""
        } else {
            // first, identify the top 3 tables in size
            val top3Tables = cluster.schema.tables.sortedByDescending { it.totalDiskSpace }.take(3)
            var i = 1
            for (table in top3Tables) {
                // for each table, we need the partition count per node
                val jmxValue = cluster.databaseVersion.estimatedRowCountMetricName()
                val nodeToPartitionCountMap = cluster.metricServer.getHistogram(
                    table.getKeyspace(),
                    table.getTableName(),
                    jmxValue,
                    "Value"
                )

                // space user per node as a map.
                val nodeToSpaceUsedMap = cluster.metricServer.getHistogram(
                    table.getKeyspace(),
                    table.getTableName(),
                    "LiveDiskSpaceUsed",
                    "Count"
                )

                // for each DC this keyspace is within, for the nodes within that DC - how balanced is the partition count? (and the space)
                // how do we tell if the keyspace is in the dc?
                val tableKeyspace = cluster.schema.keyspaces.firstOrNull { it.name == table.getKeyspace() }
                if (tableKeyspace == null) {
                    cluster.loadErrors.add(
                        LoadError(
                            "All",
                            "Unable to find the keyspace details for table ${table.name}. Data balance calculations did not include this table."
                        )
                    )
                    continue
                }

                if (tableKeyspace.strategy == "SimpleStrategy") {
                    val markdownResults = MarkdownTable(
                        "Node",
                        "Number of Partitions",
                        "% of Total Partitions",
                        "Live Data Size",
                        "% of Total Size"
                    )

                    // all nodes are considered together for the purposes of balance
                    for (node in nodeToPartitionCountMap.getData().toList().sortedByDescending { (_, value) -> value }.toMap()) {
                        val nodeSpaceUsedValue = nodeToSpaceUsedMap.getByNodeOrDefault(node.key,-1.0)
                        markdownResults.addRow()
                            .addField(node.key)
                            .addField(node.value.toLong().toHumanCount().toString())
                            .addField(String.format("%.2f", (node.value / nodeToPartitionCountMap.sum()) * 100.0))
                            .addField(ByteCountHelper.humanReadableByteCount(nodeSpaceUsedValue))
                            .addField(String.format("%.2f", (nodeSpaceUsedValue  / nodeToSpaceUsedMap.sum()) * 100.0))
                    }
                    args[i.toString() + "_name"] = table.name
                    args[i.toString() + "_table"] = markdownResults
                    i++
                }
                if (tableKeyspace.strategy == "NetworkTopologyStrategy") {
                    val markdownResults = MarkdownTable(
                        "Node",
                        "DC",
                        "Number of Partitions",
                        "% of Total Partitions",
                        "Live Data Size",
                        "% of Total Size"
                    )

                    // the calculation is now on a per DC basis.
                    for (dc in tableKeyspace.options.map { it.first }.distinct().sorted()) {
                        val nodesInDc = cluster.getNodesFromDC(dc)

                        val filteredNodeToPartitionCountMap = nodeToPartitionCountMap.getData().filter { nodesInDc.map { n -> n.hostname }.contains(it.key)}

                        val filteredNodeToSpaceUsedMap = nodeToSpaceUsedMap.getData().filter { nodesInDc.map { n -> n.hostname }.contains(it.key) }

                        // now run the calcs for these nodes
                        val totalPartitionCountInDC = filteredNodeToPartitionCountMap.map { it.value }.sum()
                        val totalSizeInDC = filteredNodeToSpaceUsedMap.map { it.value }.sum()

                        val sortedPartitionCountMap = filteredNodeToPartitionCountMap.toList().sortedByDescending { (_,value) -> value }.toMap()
                        for (node in sortedPartitionCountMap) {
                            val nodeSizeValue = filteredNodeToSpaceUsedMap.getOrDefault(node.key, -1.0)
                            markdownResults.addRow()
                                .addField(node.key)
                                .addField(dc)
                                .addField(node.value.toLong().toHumanCount().toString())
                                .addField(
                                    String.format(
                                        "%.2f",
                                        (node.value / totalPartitionCountInDC) * 100.0
                                    )
                                )
                                .addField(ByteCountHelper.humanReadableByteCount(nodeSizeValue))
                                .addField(String.format("%.2f", (nodeSizeValue / totalSizeInDC) * 100.0))
                        }
                    }
                    args[i.toString() + "_name"] = table.name
                    args[i.toString() + "_table"] = markdownResults
                    i++
                }
            }
            return compileAndExecute("operations/operations_table_partition_balance.md", args)
        }
    }

    internal fun isDataLoadBalanced(cluster: Cluster): Boolean {
        // For Each DC
        // We want the data load per node to be within 25% - which is pretty generous
        val dcList = cluster.getDCNames()
        for (dc in dcList) {
            val nodeList = cluster.getNodesFromDC(dc)
            val maxLoadInDC = nodeList.maxByOrNull { it.info.loadInBytes }?.info?.loadInBytes ?: 0
            val minLoadInDC = nodeList.minByOrNull { it.info.loadInBytes }?.info?.loadInBytes ?: 0
            // If 75% of the maximum is still higher than the minimum, then the DC is unbalanced, so the cluster is.
            if ( maxLoadInDC.toDouble() * 0.75 > minLoadInDC.toDouble() ) {
                return false
            }
        }
        // no DC failed the max / min check, assume balanced.
        return true
    }
}