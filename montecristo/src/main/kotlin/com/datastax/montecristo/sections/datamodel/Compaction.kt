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

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.CompactionDetail
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class Compaction : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val tables = cluster.schema.getUserTables().sortedByDescending { x -> x.getOperationsCount() }

        val tablesMarkdown =  MarkdownTable(" Table","Strategy","Reads","Writes","Operations","R:W Ratio","Avg Space Used","p95 SStables per read")
        tables.sortedByDescending { it.writes.num + it.reads.num }.forEach {
            val sstablePerReadMetric = it.sstablesPerReadHistogram.p95.max() ?: 0.0
            val rowColor = when {
                sstablePerReadMetric >= 10.0 -> {
                    "red"
                }
                sstablePerReadMetric >= 5.0 -> {
                    "orange"
                }
                else -> {
                    ""
                }
            }
            tablesMarkdown.addRow()
                .addField(processValueWithColour(it.name, rowColor))
                .addField(processValueWithColour(it.compactionStrategy.shortName, rowColor))
                .addField(processValueWithColour(it.reads.toString(), rowColor))
                .addField(processValueWithColour(it.writes.toString(), rowColor))
                .addField(processValueWithColour(it.operations.toString(), rowColor))
                .addField(processValueWithColour(it.getRWRatioHuman(), rowColor))
                .addField(processValueWithColour(it.liveDiskSpaceUsed.count.averageBytesAsHumanReadable(), rowColor))
                .addField(processValueWithColour(it.sstablesPerReadHistogram.p95.maxFormatted2DP(), rowColor))
        }

        val toSTCS = MarkdownTable("Table", "Explanation")
        val toLCS =MarkdownTable("Table", "Explanation")
        val toTWCS = MarkdownTable("Table", "Explanation")

        for (table in tables) {
            // are we on the right strategy
            table.liveDiskSpaceUsed.count.averageAsHumanReadable()
            val recommended = getOptimalCompactionStrategy(table)
            if (recommended.shortName != table.compactionStrategy.shortName) {
                when (recommended.shortName) {
                    "STCS" -> toSTCS.addRow().addField(table.name).addField("R:W ratio of ${table.getRWRatioHuman()}")
                    "TWCS" -> toTWCS.addRow().addField(table.name)
                            .addField(if (table.compactionStrategy.shortName == "DTCS") {
                                     "Using DTCS"
                                } else {
                                    "Identified as potential time series"
                                })
                    "LCS" -> toLCS.addRow().addField(table.name).addField("R:W ratio of ${table.getRWRatioHuman()} and/or SSTables per read p95 is ${table.sstablesPerReadHistogram.p95.max()}")
                }
            }
        }

        if (toTWCS.rows.isNotEmpty()) {
            args["toTWCS"] = toTWCS.toString()
            recs.near(RecommendationType.DATAMODEL, "We recommend evaluating changing the compaction strategy to TWCS for ${toTWCS.rows.size} ${"table.".plural("tables.", toTWCS.rows.size)}")
        }

        if (toLCS.rows.isNotEmpty()) {
            args["toLCS"] = toLCS.toString()
            recs.near(RecommendationType.DATAMODEL, "We recommend evaluating changing the compaction strategy to LCS for ${toLCS.rows.size} ${"table.".plural("tables.", toTWCS.rows.size)}")
        }

        if (toSTCS.rows.isNotEmpty()) {
            args["toSTCS"] = toSTCS.toString()
            recs.near(RecommendationType.DATAMODEL, "We recommend evaluating changing the compaction strategy to STCS for ${toSTCS.rows.size} ${"table.".plural("tables.", toTWCS.rows.size)}")
        }

        val tableWithHighSSTablePerRead = tables.filter { it.sstablesPerReadHistogram.p95.max() ?: 0.0 >= 5.0 }
        if (tableWithHighSSTablePerRead.isNotEmpty()) {
            recs.near(RecommendationType.DATAMODEL, "There ${"is".plural("are", tableWithHighSSTablePerRead.size)} ${tableWithHighSSTablePerRead.size} ${"table".plural("tables", tableWithHighSSTablePerRead.size)} with the p95 SSTable Per Read histogram above 5. This indicates an issue in the data model or compaction strategy choice for the ${"table".plural("tables", tableWithHighSSTablePerRead.size)}. We recommend reviewing the ${"table".plural("tables", tableWithHighSSTablePerRead.size)}, usage and compaction strategy.")
        }

        val tablesWithPending = tables.filter { it.pendingCompactions.value.sum() > 0 }
        if (tablesWithPending.isNotEmpty()) {
            val tablesWithPendingMd = MarkdownTable("Table", "Total Pending Compactions")
            tablesWithPending.sortedByDescending { it.pendingCompactions.value.sum() }.forEach {
                tablesWithPendingMd.addRow()
                    .addField(it.name)
                    .addField(it.pendingCompactions.value.sum())
            }
            args["tablesWithPendingMd"] = tablesWithPendingMd.toString()
        }

        val totalPendingAverage = tablesWithPending.sumOf { it.pendingCompactions.value.average() }
        if (totalPendingAverage > 20) {
            recs.immediate(RecommendationType.DATAMODEL, "Increase compaction_throughput to keep up with pending compactions.")
        }

        val lcsTablesWithNonDefaultSSTableSize = tables.filter { it.compactionStrategy.shortName == "LCS"
                                                                && (it.compactionStrategy.getOption("sstable_size_in_mb", cluster.databaseVersion.lcsDefaultSSTableSize()) != cluster.databaseVersion.lcsDefaultSSTableSize()
                                                                    || it.compactionStrategy.getOption("fanout_size", cluster.databaseVersion.lcsDefaultFanOutSize()) != cluster.databaseVersion.lcsDefaultFanOutSize())}
        if (lcsTablesWithNonDefaultSSTableSize.isNotEmpty()) {
            recs.near(RecommendationType.DATAMODEL, "There ${"is".plural("are", lcsTablesWithNonDefaultSSTableSize.size)} ${lcsTablesWithNonDefaultSSTableSize.size} ${"table".plural("tables", lcsTablesWithNonDefaultSSTableSize.size)} using the LCS strategy with a non-default sstable_per_mb or fanout_size value. We recommend reviewing the reason for using non-default values.")
        }

        args["tables"] = tablesMarkdown

        return compileAndExecute("datamodel/datamodel_compaction.md", args)
    }


    // This was moved out of the data model into the document section, its a rule recommendation, not a data model object
    // 90% reads we go to LCS and more than 5 SSTables per read at p95
    // DTCS we go to TWCS
    // LCS stays as LCS
    // all others (for now) we go to STCS
    // there are other cases, this is by no means final
    // let's discuss in detail later
    internal fun getOptimalCompactionStrategy(table: Table): CompactionDetail {

        // If the table is using DSE CFS compaction, then do not recommend to change it.
        if (table.compactionStrategy.shortName == "CFS") {
            return CompactionDetail.cfs()
        }

        // assuming they have a time series.  it's possible they are using DTCS incorrectly, beware!
        if (table.compactionStrategy.shortName == "DTCS") {
            return CompactionDetail.twcs()
        }

        if (table.readLatency.count.sum() > table.writeLatency.count.sum() * 10
            && table.sstablesPerReadHistogram.p95.max() != null
            && table.sstablesPerReadHistogram.p95.max()!!.toInt() > 5
        ) {
            return CompactionDetail.lcs()
        }

        if (table.sstablesPerReadHistogram.p95.max() != null && table.sstablesPerReadHistogram.p95.max()!!
                .toInt() >= 7
        ) {
            return CompactionDetail.lcs()
        }

        // We don't have a firstClusteringKey data type, so we have to skip checking it.
        if (table.compactionStrategy.shortName != "TWCS" && !table.isMV) {
            if (table.clusteringColumns.isNotEmpty() && table.firstClusteringKeyIsTimestamp()) {
                return CompactionDetail.twcs()
            }
        }

        // If the table already is a TWCS, we don't want it to just default to an STCS recommendation
        // As long as a part of the partition key is date based, or the first cluster column is, then leave it as TWCS
        // for now instead of letting it hit the STCS default recommendation.
        if (table.compactionStrategy.shortName == "TWCS") {
            if ((table.clusteringColumns.isNotEmpty() && table.firstClusteringKeyIsTimestamp()) || (table.partitionKeyIncludesTimestamp())) {
                return CompactionDetail.twcs()
            }
        }

        // If the strategy is currently LCS, return that, do not change an LCS to STCS as optimal.
        return if (table.compactionStrategy.shortName == "LCS") {
            CompactionDetail.lcs()
        } else {
            CompactionDetail.stcs()
        }
    }
}
