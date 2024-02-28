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
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class SSTableCounts : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // only interested in STCS tables - strip out any double quotes that have been used in the schema generation, the metrics won't contain those double quotes.
        val stcsTables = cluster.schema.getUserTables().filter { it.compactionStrategy.shortName == "STCS" }.map{ it.name.replace("\"","")}.toSet()
        // per node we need the list of tables with a live ss table count greater than the threshold.
        val highSSTableCounts = cluster.metricServer.getSStableCounts(UPPER_SSTABLE_COUNT_LIMIT)
        // we now need to filter the high ss table count vs the STCS list, bit tricky since we are filter a list of objects
        // based on a name
        val tablesToReportOn = highSSTableCounts.filter { stcsTables.contains(it.keyspace + "." + it.tableName) }
        // sort the data into something reasonable, e.g. by node, by keyspace, by table
        val sortedTablesToReportOn = tablesToReportOn.sortedWith( compareBy { it.node + "." + it.keyspace + "." + it.tableName })
        val ssTableData = MarkdownTable("Node", "Keyspace", "Table Name", "SSTable Count")
                .orMessage("There are no tables with excessive SSTable counts.")

        sortedTablesToReportOn.forEach {

            ssTableData.addRow()
                    .addField(it.node)
                    .addField(it.keyspace)
                    .addField(it.tableName)
                    .addField(it.tableCount)
        }
        val numberOfTablesWithIssues = sortedTablesToReportOn.groupingBy { it.keyspace + "." + it.tableName }.eachCount().size
        args["sstablecounts"] = ssTableData.toString()

        if (tablesToReportOn.isNotEmpty())
            recs.near(RecommendationType.DATAMODEL,"We recommend that you review the compaction configuration and settings for $numberOfTablesWithIssues table(s) to reduce the SSTable count.")

        return compileAndExecute("datamodel/datamodel_sstable_count.md", args)
    }

    companion object {
        private const val UPPER_SSTABLE_COUNT_LIMIT = 20
    }
}