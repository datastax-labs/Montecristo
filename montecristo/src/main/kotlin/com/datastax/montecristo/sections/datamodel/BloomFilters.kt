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

import com.datastax.montecristo.helpers.humanBytes
import com.datastax.montecristo.helpers.toPercent
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class BloomFilters : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val md = MarkdownTable("Table", "Reads (Total)", "False Positive Chance", "Recent FP Rate (avg)", "Offheap Memory Used (mean)", "Disk Space (mean)")
                .orMessage("No tables exist for bloom filters.")

        var badRecentFPCount = 0
        cluster.schema.tables.sortedByDescending { it.readLatency.count.sum() }.forEach {
            val recentFPRate = if (!it.recentBloomFilterFalseRatio.value.isEmpty()) it.recentBloomFilterFalseRatio.value.average() else 0.0
            val offHeapMemoryUsed = if (!it.bloomFilterOffHeapMemoryUsed.value.isEmpty()) it.bloomFilterOffHeapMemoryUsed.value.average().humanBytes() else "0"
            val diskSpaceUsed = if (!it.bloomFilterDiskSpaceUsed.value.isEmpty()) it.bloomFilterDiskSpaceUsed.value.average().humanBytes() else "0"
            // if the recent fp rate is double or higher, its red
            val rowColor = when {
                recentFPRate >= (it.fpChance * 2.0) -> {
                    badRecentFPCount++
                    "red"
                    // if its more than the fpChance, its orange
                }
                recentFPRate > it.fpChance -> {
                    "orange"
                }
                else -> {
                    ""
                }
            }
            md.addRow()
                    .addField(processValueWithColour(it.name, rowColor))
                    .addField(processValueWithColour(it.reads.toString(), rowColor))
                    .addField(processValueWithColour(it.fpChance.toPercent(""), rowColor))
                    .addField(processValueWithColour(recentFPRate.toPercent("") , rowColor))
                    .addField(processValueWithColour(offHeapMemoryUsed, rowColor))
                    .addField(processValueWithColour(diskSpaceUsed, rowColor))
        }
        args["overview"] = md.toString()

        if (badRecentFPCount > 0) {0
            recs.near(RecommendationType.DATAMODEL,"$badRecentFPCount ${"table is".plural("tables are", badRecentFPCount)} encountering far too many false positives when reading the bloom filters to locate data. We recommend further investigation and tuning of the bloom filters.")
        }
        return compileAndExecute("datamodel/datamodel_bloom_filters.md", args)
    }
}