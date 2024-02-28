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
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class Caches : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        val nodetoolInfoExample = cluster.nodes.first().info.rawContent

        // ignoring system tables in this.
        val tablesWithRowCaching = cluster.schema.getUserTables().filter { table -> table.caching.rows != "NONE" }
        val rowCacheTable = MarkdownTable("Table", "Cache Hits", "Cache Misses", "Hit Rate","p99 Read Latency (ms)","R:W Ratio").orMessage("No tables have row cache enabled.")
        tablesWithRowCaching.forEach {
            rowCacheTable.addRow()
                .addField(it.name)
                .addField(it.rowCacheHit.count.sumAsHumanReadable())
                .addField(it.rowCacheMiss.count.sumAsHumanReadable())
                .addField(it.getRowCacheRatioHuman())
                .addField(it.getReadLatencyP99())
                .addField(it.getRWRatio())
        }

        val keyCacheTable = MarkdownTable("Setting","Config Value","Description")
        keyCacheTable.addRow()
            .addField("key_cache_keys_to_save")
            .addField(cluster.getSetting("key_cache_keys_to_save", ConfigSource.CASS).getSingleValue())
            .addField("Number of keys to cache")
        keyCacheTable.addRow()
            .addField("key_cache_save_period")
            .addField(cluster.getSetting("key_cache_save_period", ConfigSource.CASS).getSingleValue())
            .addField("Frequency to save key cache")

        val counterCacheTable = MarkdownTable("Setting","Config Value","Impact")
        counterCacheTable.addRow()
            .addField("Counter Cache Size")
            .addField(cluster.getSetting("counter_cache_size_in_mb", ConfigSource.CASS).getSingleValue())
            .addField("Total Counter Cache In MB")
        counterCacheTable.addRow()
            .addField("Save Period")
            .addField(cluster.getSetting("counter_cache_save_period", ConfigSource.CASS).getSingleValue())
            .addField("Cache flush frequency")

        val tablesUsingCounters = cluster.schema.getUserTables().map { Pair(it.name,  it.fields.filter { field -> field.value.dataTypeString == "counter"}.size ) }
            .filter { it.second > 0}
            .sortedByDescending { it.second }
        val tablesWithCountersMd = MarkdownTable("Table", "Number of Counter Columns").orMessage("No tables were using counters.")
        tablesUsingCounters.forEach {
            tablesWithCountersMd.addRow().addField(it.first).addField(it.second)
        }

        val pageCacheUsage = cluster.nodes.map { it.osConfiguration.memInfo.cached!! }
        // need to convert to bytes, memory usage is reported in kB
        val pageCacheMin = pageCacheUsage.minOrNull()!! * 1024
        val pageCacheMax = pageCacheUsage.maxOrNull()!! * 1024

        args["rowCacheTable"] = rowCacheTable
        args["nodetoolInfoExample"] = nodetoolInfoExample.trim().replace("(?m)\\s*nodetool rc=0".toRegex(), "")
        args["keyCacheTable"] = keyCacheTable
        args["counterCacheTable"] = counterCacheTable
        args["pageCacheMin"] = ByteCountHelper.humanReadableByteCount(pageCacheMin)
        args["pageCacheMax"] = ByteCountHelper.humanReadableByteCount(pageCacheMax)
        args["tablesWithCounters"] = tablesWithCountersMd.toString()

        if (cluster.nodes.first().cassandraYaml.isRowCacheEnabled()) {
            recs.near(RecommendationType.DATAMODEL,"We recommend setting the `row_cache_in_mb` to 0 to disable row caching and instead utilize the operating system page cache which is more effective.")
        }
        return compileAndExecute("datamodel/datamodel_caches.md", args)
    }
}