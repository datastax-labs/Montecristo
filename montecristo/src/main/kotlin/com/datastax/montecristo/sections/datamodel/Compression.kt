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

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.helpers.humanBytes
import com.datastax.montecristo.helpers.round
import com.datastax.montecristo.helpers.toPercent
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class Compression : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val tables = cluster.schema.getUserTables() //exclude system tables from the checks
        val compressionData = MarkdownTable("Table", "Algorithm", "Chunk Length", "R:W Ratio", "Partition Size (mean)", "Total Space Used by Table (Compressed)", "Compression % Saved")
                .orMessage("There are no tables using compression.")

        // sorted by reads only, read heavy tables are of the greatest interest when looking at compression chunk sizes
        tables.sortedByDescending { it.reads.num }.forEach {
            var rowColor = ""
            val compressionRatio = if (it.compressionRatio.value.max()?: 0.0 > 0.0) {
                // This can go negative, e.g. the compressed file is larger than the original data
                val preFormattedRatio = (1.0 - it.compressionRatio.value.max()!!)
                if (preFormattedRatio < 0) {
                    rowColor = "red"
                } else if (preFormattedRatio < 0.1) {
                    rowColor = "orange"
                }
                preFormattedRatio.toPercent("")
            } else {
                "-" // below 0.0 such as the -1.0 value
            }
            compressionData.addRow()
                    .addField(processValueWithColour(it.name, rowColor))
                    .addField(processValueWithColour(it.compression.getShortName(), rowColor))
                    .addField(processValueWithColour(it.compression.getChunkLength(), rowColor))
                    .addField(processValueWithColour(it.getRWRatioHuman(), rowColor))
                    .addField(processValueWithColour(it.meanPartitionSize.value.averageBytesAsHumanReadable(), rowColor))
                    .addField(processValueWithColour(it.liveDiskSpaceUsed.count.sum().humanBytes() , rowColor))
                    .addField(processValueWithColour(compressionRatio, rowColor))
        }

        val countOfTablesWithBadCompression = tables.filter { it.compressionRatio.value.max()?:0.0 >= BAD_COMPRESSION }.size
        if (countOfTablesWithBadCompression > 0) {
            recs.near(RecommendationType.DATAMODEL,"$countOfTablesWithBadCompression table(s) are using compression but not gaining any advantage from doing so. We recommend reviewing and removing compression on these tables.")
        }

        val readHeavy = MarkdownTable("Table", "Read Latency (ms, p99)", "SSTables Per Read (p99)",  "Partition Size (mean)", "Partition Size (max)", "R:W Ratio", "Space")

        tables.filter { it.getRWRatio() > 10 }
                .sortedByDescending { it.readLatency.count.sum() }
                .forEach {
            readHeavy.addRow()
                    .addField(it.name)
                    //.addField(Utils.round(it.readLatency.p99.max() ?: 0.0 / 1000.0))
                    .addField(Utils.round((it.readLatency.p99.max() ?: 0.0) / 1000.0 ))
                    .addField(if (it.sstablesPerReadHistogram.p99.isEmpty()) { "" } else { it.sstablesPerReadHistogram.p99.average().round() } )
                    .addField(it.meanPartitionSize.value.averageBytesAsHumanReadable())
                    .addField(it.maxPartitionSize.value.averageBytesAsHumanReadable())
                    .addField(it.getRWRatioHuman())
                    .addField(it.totalDiskSpaceUsed.count.sum().humanBytes())
        }
        val args = super.createDocArgs(cluster)
        args["readHeavy"] = readHeavy.toString()
        args["tablesUsingCompression"] = compressionData.toString()
        args["showChunkLengthKBNote"] = cluster.databaseVersion.showChunkLengthKBNote()

        // List the top 10
        val topTablesWithHighChunkLength = tables.filter { it.compression.getChunkLength().toInt() > 16 }.sortedByDescending { it.readLatency.count.sum() }
        val defaultUsed = topTablesWithHighChunkLength.any { it.compression.getChunkLength().toInt() == 64 }

        if (topTablesWithHighChunkLength.isNotEmpty()) {
            val defaultMessage = if (defaultUsed) " Several tables are using the default chunk length of 64kb which in most cases involves more I/O" +
                    " usage than necessary." else ""
            recs.immediate(RecommendationType.DATAMODEL,"We recommend lowering the compression chunk length down to 16kb," +
                    " for all the tables that have a high read traffic, where the mean partition size is smaller than 16kb." + defaultMessage +
                    " You can do this by altering the schema and running a subsequent" +
                    " `nodetool upgradesstables -a` to rewrite all SSTables to disk.")
        }
        return compileAndExecute("datamodel/datamodel_compression.md", args)
    }

    companion object {
        private const val BAD_COMPRESSION = 0.9
    }
}
