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

package com.datastax.montecristo.sections.astra

import com.datastax.montecristo.helpers.ByteCountHelper.humanReadableByteCount
import com.datastax.montecristo.helpers.toHumanCount
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.NodeTableReadCount
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Keyspace
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MarkdownTable
import org.slf4j.LoggerFactory
import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.roundToLong

class Operations : DocumentSection {

    private val logger = LoggerFactory.getLogger(this::class.java)
    data class ConsistencyLevelResults(val one: Long, val localQuorum: Long, val quorum: Long, val all: Long)
    data class WriteOperationResults (val writes : Long, val totalSpaceUsedUncompressed : Double, val nonRFSpaceUsedCompressed : Double,  val nonRFSpaceUsedUncompressed : Double)

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        try {
            val operationsResults = MarkdownTable(
                "Table",
                "Read - One - Ops/hr",
                "Read - Local Quorum - Ops/hr",
                "Read - Quorum - Ops/hr",
                "Read - All - Ops/hr",
                "Read - Co-ordinator / Hr",
                "Write - Ops/ Hr",
                "Total Data (1 Replica Only - Compressed)",
                "Total Data (1 Replica Only - Uncompressed)",
                "Total Data (All Nodes - Uncompressed)",
                "Total Rows (All Nodes)",
                "Calculated Average Row Size - Uncompressed"
            ).orMessage("**WARNING ** \n1 or more nodes have no uptime value, which prevents hourly rate calculations. (or you have asked to calculate operations on a schema with no user objects)")

            // the client request metrics (if we have JMX we have them, otherwise they will be empty) - will give us co-ordinator counts per node / table
            val clientRequestMetrics = cluster.metricServer.getClientRequestMetrics()
            val tableList = cluster.schema.getUserTables()
            val keyspaceList = cluster.schema.getUserKeyspaces()
            if (cluster.nodes.any { it.info.upTimeInSeconds == null }) {
                logger.error("Astra Operations cannot be calculated due to missing Uptime values")
            } else {

                // Reads
                val tableReadResults = calculateTableReadOperations(tableList, keyspaceList, cluster)
                val tableRowSizes = calculateTableRowSizes(tableList, keyspaceList, cluster)
                val tableWriteResults = calculateTableWriteOperations(tableList, keyspaceList, cluster)

                var totalOne: Long = 0
                var totalLQ: Long = 0
                var totalQ: Long = 0
                var totalAll: Long = 0
                var totalCoord: Long = 0
                var totalWrites: Long = 0

                var totalSize1ReplicaCompressed = 0.0
                var totalSize1ReplicaUncompressed = 0.0
                var totalSizeAllReplica = 0.0

                val sortedTableReadResults = tableReadResults.toList()
                    .sortedByDescending { it.second.localQuorum + tableWriteResults.getValue(it.first).writes }

                sortedTableReadResults.forEach {
                    val coord = getClientRequestOperations(cluster, clientRequestMetrics, it.first)
                    val writesPerHr = tableWriteResults.getValue(it.first).writes
                    val totalSpaceUsed1ReplicaCompressed =
                        (tableWriteResults.getValue(it.first).nonRFSpaceUsedCompressed.takeIf { v -> !v.isNaN() }
                            ?: 0.0)
                    val totalSpaceUsed1ReplicaUncompressed =
                        (tableWriteResults.getValue(it.first).nonRFSpaceUsedUncompressed.takeIf { v -> !v.isNaN() }
                            ?: 0.0)
                    val totalSpaceUsedAllReplicas =
                        (tableWriteResults.getValue(it.first).totalSpaceUsedUncompressed.takeIf { v -> !v.isNaN() }
                            ?: 0.0)
                    val totalRows = cluster.nodes.sumOf { node -> node.ssTableStats.getRowCount(it.first.replace("\"","")) }
                    val avgRowSize = tableRowSizes.getOrDefault(it.first, 0).toDouble()

                    operationsResults.addRow().addField(it.first)
                        .addField(it.second.one.toString())
                        .addField(it.second.localQuorum.toString())
                        .addField(it.second.quorum.toString())
                        .addField(it.second.all.toString())
                        .addField(coord)
                        .addField(writesPerHr)
                        .addField(humanReadableByteCount(totalSpaceUsed1ReplicaCompressed))
                        .addField(humanReadableByteCount(totalSpaceUsed1ReplicaUncompressed))
                        .addField(humanReadableByteCount(totalSpaceUsedAllReplicas))
                        .addField(totalRows.toHumanCount())
                        .addField(avgRowSize)
                    totalOne += it.second.one
                    totalLQ += it.second.localQuorum
                    totalQ += it.second.quorum
                    totalAll += it.second.all
                    totalCoord += coord
                    totalWrites += writesPerHr
                    totalSize1ReplicaCompressed += totalSpaceUsed1ReplicaCompressed
                    totalSize1ReplicaUncompressed += totalSpaceUsed1ReplicaUncompressed
                    totalSizeAllReplica += totalSpaceUsedAllReplicas
                }

                operationsResults.addRow().addField("Total")
                    .addField(totalOne.toString())
                    .addField(totalLQ.toString())
                    .addField(totalQ.toString())
                    .addField(totalAll.toString())
                    .addField(totalCoord.toString())
                    .addField(totalWrites)
                    .addField(humanReadableByteCount(totalSize1ReplicaCompressed))
                    .addField(humanReadableByteCount(totalSize1ReplicaUncompressed))
                    .addField(humanReadableByteCount(totalSizeAllReplica))
            }
            args["readOperations"] = operationsResults.toString()
        } catch (e: Exception) {
            logger.error("Failed generating operations overview", e)
        }

        return compileAndExecute("astra/astra_operations.md", args)
    }

    internal fun calculateTableReadOperations(tableList: List<Table>, keyspaceList: List<Keyspace>, cluster: Cluster): Map<String, ConsistencyLevelResults> {
        // Reads - this is the complex one, the complexities are:
        // Multi DC vs Single DC
        // Async Read Repair chance (local and global)
        // paxos
        // speculative retries - TODO : leaving this out for now
        // we must calculate multiple values - based on potential CL of the caller

        // Map of Table to Node,WriteCount
        val tableReadsPerNode = tableList.associateWith { it.readLatency.count.getData() }

        return tableReadsPerNode.map { tableNodeCounts ->
            val table = tableNodeCounts.key
            val readsPerNode = tableNodeCounts.value.toList()

            // global read repair. If Set then x% of the reads on this node were caused by the global read repair.
            // dc_local read repair. Is set then y% of the reads on this node were cause by the local read repair, but x% of the y% are upgraded as global
            // e.g. if both percentages are 10%, then of 100 reads, 10% get a global RR, 10% get a local RR - but 10% of the 10% get both - and the global wins.
            // if its a single DC cluster, global is zero'ed regardless
            val globalRRPercentage = getGlobalRR(cluster, table)
            // local RR percentage - remove the percentage that will get promoted to global
            val localRRPercentage = ((table.dcLocalReadRepair.toDoubleOrNull() ?: 0.0) * (1.0 - globalRRPercentage))

            // get the keyspace object of the table, we need it for the RF
            val ks = keyspaceList.firstOrNull { ks -> ks.name == tableNodeCounts.key.getKeyspace() }
            if (ks != null) {
                val totalRF = getTotalRF(ks)
                // find out how many paxos operations hit this table and * 3 for the 3 stages which all cause reads.
                val numberOfPaxosOperations = table.casPrepareLatency.count.getData().map { Pair(it.key, it.value.roundToLong()) }.toMap()

                // we have the counts per node, but depending on the CL used, this has to be interpreted differently
                // the impact of the rr / dc_local_rr also changes things

                // CL ONE
                // Sum up all the reads as the total
                // every read is independent regardless of DCs and RF, only have to account for RR
                val clOneOperations = readsPerNode.sumOf {
                    calculateReads(
                        cluster,
                        ks,
                        it.first,
                        it.second,
                        globalRRPercentage,
                        localRRPercentage,
                        1,
                        numberOfPaxosOperations.getOrDefault(it.first, 0)
                    )
                }.roundToLong()

                // CL Local Quorum
                // Process reads per node, DC has impact - then divide by Quorum (account for RR) - add the DCs together
                val clLocalQuorumOperations = readsPerNode.sumOf {
                    val replicasPerRead = convertRFToQuorum(getKeyspaceRFForDC(ks, getNodeDC(cluster, it.first)))
                    calculateReads(
                        cluster,
                        ks,
                        it.first,
                        it.second,
                        globalRRPercentage,
                        localRRPercentage,
                        replicasPerRead,
                        numberOfPaxosOperations.getOrDefault(it.first, 0)
                    )
                }.roundToLong()

                // CL Quorum
                // Process reads per node, DC has no impact - then divide by Quorum (account for RR)
                val clQuorumOperations = readsPerNode.sumOf {
                    calculateReads(
                        cluster,
                        ks,
                        it.first,
                        it.second,
                        globalRRPercentage,
                        localRRPercentage,
                        convertRFToQuorum(totalRF),
                        numberOfPaxosOperations.getOrDefault(it.first, 0)
                    )
                }.roundToLong()

                // CL ALL
                // easy one - every read is to all replicas, read repair has no impact, DC's have no impact
                // sum of the reads divided by the total RF, e.g. 6 nodes, 1m reads each, RF=3 is 2m operations, each operation makes 3 reads.
                val clAllOperations = readsPerNode.sumOf {
                    val localRF = getKeyspaceRFForDC(ks, getNodeDC(cluster, it.first))
                    if (localRF > 0) {
                        val uptime = getNodeUptimeInHours(cluster, it.first)

                        val operationsPerHour =
                            (it.second - (numberOfPaxosOperations.getOrDefault(it.first, 0) * 2)) / uptime
                        operationsPerHour / totalRF.toDouble()
                    } else {
                        0.0
                    }
                }.roundToLong()

                Pair(tableNodeCounts.key.name, ConsistencyLevelResults(clOneOperations, clLocalQuorumOperations, clQuorumOperations, clAllOperations))

            } else {
                // TO DO : we can't find the keyspace for the table - is there a better alternative than -1 to denote a problem?
                Pair(tableNodeCounts.key.name, ConsistencyLevelResults(-1, -1, -1, -1))
            }
        }.toMap()
    }

    private fun calculateReads(cluster: Cluster, ks: Keyspace, nodeName: String, reads: Double, globalRRPercentage: Double, localRRPercentage: Double, replicasPerRead: Int, paxosReads: Long): Double {
        val totalRF = getTotalRF(ks)
        val localRF = getKeyspaceRFForDC(ks, getNodeDC(cluster, nodeName))
        val uptime = getNodeUptimeInHours(cluster, nodeName)

        if (localRF > 0) {
            // ok, this is complex!
            // Global reads read all copies, so its % * total rf reads
            // local reads local copies, so its local rf * RF reads
            // the remaining reads are based on replicas per read
            // we only care about the remaining reads - but we don't know what percentage this is, its not as simple as saying 90% normal 10% RR, because the RR creates more reads,
            // e.g. 100 reads, 10% RR at CL one, = 90 reads of 1 = 90 and 10 reads of 3 = 30 - total 120 reads. Normal reads is 90/120 of the total e.g. 75%
            // 100 reads, 10% RR at CL Quorum = 90 reads of 2 = 180 and 10 reads of 3 = 30 - total of 210 reads. Normal reads is 180/210 of the total e.g. 85.7%
            // at 0 read repair, its then divided by the replica per read only (since its 1 * replicas per read)
            val denominator = if (replicasPerRead <= localRF) {
                val quorumReadsPercentage = 1 - (globalRRPercentage + localRRPercentage)
                (globalRRPercentage * totalRF) + (localRRPercentage * localRF) + (quorumReadsPercentage * replicasPerRead)
            } else {
                // this is a scenario where we are reading more copies than exists on this node's DCs, the local read repair starts making less sense since it would indicate less reads than actually occurred
                // when doing a local RR compared to a quorum operation. As it stands, it is most likely that the local read is already inside the normal quorum read since the quorum
                // would of read from local nodes before cross-dc reads. Local RR is ignored in this scenario of performing a Quorum on a multi DC with local RR
                val quorumReadsPercentage = 1 - (globalRRPercentage)
                (globalRRPercentage * totalRF) + (quorumReadsPercentage * replicasPerRead)
            }
            // all other reads where performed at the replicaPerRead level, this could be 1, Quorum, All.(numerically)
            // paxos reads are a CAS Overhead and must come off after any ReadRepair effect, we know they are not impacted by RR
            // because paxos has multiple stages, we are going to work on the basis that a paxos latency count = 3 read were generated, 2 are extra and need to be removed.
            return (((reads - paxosReads * 2)/ denominator)/ uptime)
        } else {
            // this KS is not replicated in this DC
            return 0.0
        }
    }

    internal fun calculateTableWriteOperations(tableList: List<Table>, keyspaceList: List<Keyspace>, cluster: Cluster): Map<String, WriteOperationResults> {
        // No matter what CL they write at, the writes go to all nodes, so we can ignore CL and RR for this calculation
        // we are just working back from the total writes per table per node back to 'how many actual client calls was this?'
        // per node - Sum(((writes / uptime) / totalRf))


       return tableList.associate { tableNodeCounts ->
                // get the keyspace object of the table, we need it for the RF
                val ks = keyspaceList.firstOrNull { ks -> ks.name == tableNodeCounts.getKeyspace() }
                if (ks != null) {
                    val totalRF = getTotalRF(ks)
                    val writesPerNode = tableNodeCounts.writeLatency.count.getData().toList()
                    // disk space used for this table - compression can be in effect so we have to account for it
                    val totalSpaceUsedCompressed = tableNodeCounts.liveDiskSpaceUsed.count.sum()
                    val nonRFSpaceUsedCompressed = totalSpaceUsedCompressed / totalRF
                    // remove compression
                    val compressionRatio = if (tableNodeCounts.compressionRatio.value.average() == -1.0) {
                        1.0
                    } else {
                        tableNodeCounts.compressionRatio.value.getData().filter { it.value != -1.0 }.values.average()
                    }
                    // calculate the decompressed values - allow compression to be a negative percentage in which case we get smaller by dividing by 1.x instead of 0.x
                    val totalSpaceUsedUncompressed = totalSpaceUsedCompressed / if (compressionRatio < 0) {
                        (1.0 + abs(compressionRatio))
                    } else {
                        compressionRatio
                    }
                    val nonRFSpaceUsedUncompressed = nonRFSpaceUsedCompressed / if (compressionRatio < 0) {
                        (1.0 + abs(compressionRatio))
                    } else {
                        compressionRatio
                    }

                    val tableWriteOperations = writesPerNode.fold(0.0) { total, it ->
                        val localRF = if (ks.strategy == "SimpleStrategy") {
                            getTotalRF(ks)
                        } else {
                            getKeyspaceRFForDC(ks, getNodeDC(cluster, it.first))
                        }
                        total + if (localRF > 0) {
                            (((it.second / getNodeUptimeInHours(cluster, it.first)) / totalRF.toDouble()))
                        } else {
                            // not replicated within this DC
                            0.0
                        }
                    }
                    Pair(
                        tableNodeCounts.name,
                        WriteOperationResults(
                            tableWriteOperations.roundToLong(),
                            totalSpaceUsedUncompressed,
                            nonRFSpaceUsedCompressed,
                            nonRFSpaceUsedUncompressed
                        )
                    )
                } else {
                    // TO DO : we can't find the keyspace for the table - is there a better alternative than -1 to denote a problem?
                    Pair(tableNodeCounts.name, WriteOperationResults(-1L, -1.0, -1.0, -1.0))
                }
            }
    }

    private fun getNodeDC(cluster: Cluster, nodeName: String): String {
        val node = cluster.getNode(nodeName)
        return node?.info?.dataCenter ?: "UNKNOWN"
    }

    private fun getKeyspaceRFForDC(keyspace: Keyspace, DC: String): Int {
        return keyspace.getDCSettings(DC)
    }

    private fun getTotalRF(keyspace: Keyspace): Int {
        return if (keyspace.strategy == "LocalStrategy") {
            1
        } else {
            keyspace.options.fold(0) { total, it -> total + it.second }
        }
    }

    private fun getNodeUptimeInHours(cluster: Cluster, nodeName: String): Double {
        // if we do not know the uptime, we will send back -1 - which will give a negative operations per hour prompting investigation.
        return cluster.getNode(nodeName)?.info?.getUptimeInHours()?: -1.0
    }

    private fun getGlobalRR(cluster: Cluster, table: Table): Double {
        // if this cluster is not multi DC, then the chance is zeroed since it has no impact.
        return if (cluster.isMultiDC()) {
            table.readRepair.toDoubleOrNull() ?: 0.0
        } else {
            0.0
        }
    }

    private fun convertRFToQuorum(rf: Int): Int {
        // While we might think of using Ceiling, when the RF is even, CEIL gives the wrong value e.g. RF = 4, Ceil(rf/2) = 2, Quorum is 3 not 2.
        // Floor(4/2) + 1 = 3, which is correct. For RF=3, then Floor(3/2)+1 = 2 - which is also correct
        // This also works for RF 1, 1/2 = 0.5, floor to 0, add 1.
        return floor(rf.toDouble() / 2.0).toInt() + 1
    }

    private fun getClientRequestOperations(cluster: Cluster, clientRequestMetrics: List<NodeTableReadCount>, ksAndTableName: String): Long {
        val data = clientRequestMetrics.filter { "${it.keyspace}.${it.tableName}".equals(ksAndTableName.replace("\"",""), ignoreCase = true) }
        // per node, get the figure
        return data.fold(0) { total, it ->
            total + (it.readCount / getNodeUptimeInHours(cluster, it.node)).roundToLong()
        }
    }

    private fun calculateTableRowSizes(tableList : List<Table>, keyspaceList: List<Keyspace>, cluster : Cluster) : Map<String,Long> {
        return tableList.associate { table ->
            // per table, size divided by the number of rows in total == approximation of a row size
            // If nodes are missing, its just going to show a total size which is proportionally less, and a row count proportionally less
            // e.g it will estimate based on what it has.

            val totalSpaceUsedCompressed = table.liveDiskSpaceUsed.count.sum()
            // remove compression
            val compressionRatio = if (table.compressionRatio.value.average() == -1.0) {
                1.0
            } else {
                table.compressionRatio.value.getData().filter { it.value != -1.0 }.values.average()
            }
            // calculate the decompressed values - allow compression to be a negative percentage in which case we get smaller by dividing by 1.x instead of 0.x
            val totalSpaceUsedUncompressed = totalSpaceUsedCompressed / if (compressionRatio < 0) {
                (1.0 + abs(compressionRatio))
            } else {
                compressionRatio
            }
            val rowCount = cluster.nodes.sumOf { node -> node.ssTableStats.getRowCount(table.name.replace("\"","")) }
            if (rowCount > 0) {
                Pair(table.name, totalSpaceUsedUncompressed.toLong() / rowCount)
            } else {
                Pair(table.name, 0L)
            }
        }
    }
}
