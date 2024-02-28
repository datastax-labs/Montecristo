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

import com.datastax.montecristo.helpers.round
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MarkdownTable

class TombstonesPerRead : DocumentSection {
    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val md = MarkdownTable(
            "Table",
            "Tombstones (p99)",
            "Tombstones (max)",
            "Read Latency (ms, p99)"
        )

        // there's no point in showing tombstone issues if there's ZERO tables that have tombstones being read
        val tablesWithJMXTombstones = cluster.schema.tables.filter {
            (it.tombstoneScannedHistogram.p99.min() ?: 0.0) > 10.0 || (it.tombstoneScannedHistogram.max.max()
                ?: 0.0) > 50
        }.sortedByDescending { it.tombstoneScannedHistogram.p99.max() }

        return if (tablesWithJMXTombstones.isNotEmpty()) {
            tablesWithJMXTombstones.forEach {
                md.addRow()
                    .addField(it.name)
                    .addField(
                        String.format(
                            "%.2f",
                            it.tombstoneScannedHistogram.p99.max()!!
                        )
                    ) // must be a value if there was a min
                    .addField(String.format("%.2f", it.tombstoneScannedHistogram.max.max()!!))
                    .addField(String.format("%.2f", it.readLatency.p99.average().round() / 1000.0))
            }
            args["tables"] = md.toString()
            return compileAndExecute("operations/operations_tombstones_per_read.md", args)
        } else {
            ""
        }
    }
}