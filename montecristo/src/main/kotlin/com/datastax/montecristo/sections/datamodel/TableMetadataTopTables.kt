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

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class TableMetadataTopTables : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val tables = cluster.schema.tables

        val topTablesBySize = MarkdownTable("Table", "Live Size", "Replica Reads", "Replica Writes", "Read Latency (ms, Avg)", "Read Latency (ms, p99)", "Write Latency (ms, Avg)", "Write Latency (ms, p99)")
        val top10BySize = tables.sortedByDescending { it.liveDiskSpaceUsed.count.sum() }.take(10)

        top10BySize.forEach {

            topTablesBySize.addRow()
                    .addField(it.name)
                    .addField(ByteCountHelper.humanReadableByteCount(it.liveDiskSpaceUsed.count.sum()))
                    .addField(it.reads.toString())
                    .addField(it.writes.toString())
                    .addField(roundMetric(it.readLatency.mean.max()))
                    .addField(roundMetric(it.readLatency.p99.max()))
                    .addField(roundMetric(it.writeLatency.mean.max()))
                    .addField(roundMetric(it.writeLatency.p99.max()))
        }

        val top10ByWrites = tables.sortedByDescending { it.writes.num }.take(10)
        val topTablesByWrites = MarkdownTable("Table", "Live Size", "Replica Writes", "Read Latency (ms, Avg)", "Read Latency (ms, p99)", "Write Latency (ms, Avg)", "Write Latency (ms, p99)")
        top10ByWrites.forEach {

            topTablesByWrites.addRow()
                    .addField(it.name)
                    .addField(ByteCountHelper.humanReadableByteCount(it.liveDiskSpaceUsed.count.sum()))
                    .addField(it.writes.toString())
                    .addField(roundMetric(it.readLatency.mean.max()))
                    .addField(roundMetric(it.readLatency.p99.max()))
                    .addField(roundMetric(it.writeLatency.mean.max()))
                    .addField(roundMetric(it.writeLatency.p99.max()))
        }

        val top10ByReads = tables.sortedByDescending { it.reads.num }.take(10)
        val topTablesByReads = MarkdownTable("Table", "Live Size", "Replica Reads", "Read Latency (ms, Avg)", "Read Latency (ms, p99)", "Write Latency (ms, Avg)", "Write Latency (ms, p99)")
        top10ByReads.forEach {

            topTablesByReads.addRow()
                .addField(it.name)
                .addField(ByteCountHelper.humanReadableByteCount(it.liveDiskSpaceUsed.count.sum()))
                .addField(it.reads.toString())
                .addField(roundMetric(it.readLatency.mean.max()))
                .addField(roundMetric(it.readLatency.p99.max()))
                .addField(roundMetric(it.writeLatency.mean.max()))
                .addField(roundMetric(it.writeLatency.p99.max()))
        }

        if (top10ByReads.any { it.name == "system.paxos"}) {
            recs.near(RecommendationType.DATAMODEL, "The system.paxos table is within the top 10 tables for reads, this indicates that there is a significant use of light weight transactions (LWTs). We recommend evaluating the use of LWTs and where possible, alter the design of the system to reduce or eliminate the use of them.")
        }

        if (top10ByWrites.any { it.name == "system.batches"}) {
            recs.near(RecommendationType.DATAMODEL, "The system.batches table is within the top 10 tables for writes, this indicates that there is a significant use of ${args["software"]} batches. Batches should be used only when transaction order of operation guarantee is required, as it is a performance anti-pattern. It is recommended that the reasons for so many batches be investigated, and the application potentially modified to reduce the use of batches.")
        }

        args["topTablesBySize"] = topTablesBySize.toString()
        args["topTablesByWrites"] = topTablesByWrites.toString()
        args["topTablesByReads"] = topTablesByReads.toString()

        return compileAndExecute("datamodel/datamodel_tablemetadata_top_tables.md", args)
    }

    private fun roundMetric(metricValue: Double?): String {
        return if (metricValue != null) {
            Utils.round(metricValue / 1000.0).toString()
        } else {
            "-"
        }
    }
}
