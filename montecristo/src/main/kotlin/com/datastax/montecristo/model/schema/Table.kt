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

package com.datastax.montecristo.model.schema

// we need to import everything in order to access min/max/etc
import com.datastax.montecristo.fileLoaders.parsers.schema.ClusteringKey
import com.datastax.montecristo.fileLoaders.parsers.schema.Field
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.helpers.toHumanCount
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.metrics.Histogram
import com.datastax.montecristo.model.metrics.MetricCounter
import com.datastax.montecristo.model.metrics.MetricValue
import com.datastax.montecristo.model.versions.DatabaseVersion

data class Table(
    var metricsDb: IMetricServer,
    var databaseVersion: DatabaseVersion,
    val partitionKeys: List<String>,
    val name: String,
    val fields: Map<String, Field>,
    val clusteringColumns: List<ClusteringKey>,
    var compactionStrategy: CompactionDetail,
    val gcGrace: Int,
    val readRepair: String,
    val dcLocalReadRepair: String,
    val compression: Compression,
    val caching: Caching,
    val fpChance: Double,
    val isMV: Boolean = false,
    val is2i: Boolean = false,
    val isSai: Boolean = false,
    val isSasi : Boolean = false,
    val isSearchIndex : Boolean = false,
    val autoLoadFromMetricsDB: Boolean = true,
    val originalCQL: String = "",
    val indexName: String,
    val baseTableName : String = ""

) {


    // all histograms go here, some may need to be moved because they might be MetricCounter or MetricValue
    val casPrepareLatency = createHistogram("CasPrepareLatency")
    val liveDiskSpaceUsed = createHistogram("LiveDiskSpaceUsed")
    var readLatency = createHistogram("ReadLatency")
    var sstablesPerReadHistogram = createHistogram("SSTablesPerReadHistogram")
    val tombstoneScannedHistogram = createHistogram("TombstoneScannedHistogram")
    val writeLatency = createHistogram("WriteLatency")
    // --------------- END OF HISTOGRAMS -----------------

    // ----------- VALUES ----------------
    // these metrics are just a Value from org.apache.cassandra.metrics
    val bloomFilterDiskSpaceUsed = createMetricValue("BloomFilterDiskSpaceUsed")
    val bloomFilterOffHeapMemoryUsed = createMetricValue("BloomFilterOffHeapMemoryUsed")
    val compressionRatio = createMetricValue("CompressionRatio")
    val estimatedPartitionCount = createMetricValue("EstimatedPartitionCount")
    val keyCacheHitRate = createMetricValue("KeyCacheHitRate")

    // be careful - it was MaxRowSize in older versions, MaxPartitionSize was not available
    val maxPartitionSize = createMetricValue(databaseVersion.maxPartitionSizeMetricName())

    // we lazily evaluate here because we need the correct metric for this version
    // mean partition size is used all over the place
    val meanPartitionSize = createMetricValue(databaseVersion.meanPartitionSizeMetricName())
    val memtableColumnsCount = createMetricValue("MemtableColumnsCount") // MetricValue
    val pendingCompactions = createMetricValue("PendingCompactions")
    val percentRepaired = createMetricValue("PercentRepaired")
    val bloomFilterFalsePositiveCount = createMetricValue("BloomFilterFalsePositives")
    val recentBloomFilterFalseRatio = createMetricValue("RecentBloomFilterFalseRatio")
    // ------------ END OF VALUES ----------------
    // ----------- COUNTS ----------------
    val rowCacheMiss = createMetricCounter("RowCacheMiss")
    val rowCacheHit = createMetricCounter("RowCacheHit")
    val speculativeRetries = createMetricCounter("SpeculativeRetries")
    val totalDiskSpaceUsed = createMetricCounter("TotalDiskSpaceUsed")
    val totalDiskSpace: Long
        get() = totalDiskSpaceUsed.count.sum().toLong()

    // Status calculated for templates, usually with a label
    val writes = writeLatency.count.sum().toLong().toHumanCount()
    val reads = readLatency.count.sum().toLong().toHumanCount()
    val operations = (writeLatency.count.sum().toLong() + readLatency.count.sum().toLong()).toHumanCount()

    private fun createHistogram(metricName : String) : Histogram {
        return Histogram(metricsDb, getKeyspace(), getTableName(), metricName)
    }

    private fun createMetricValue(metricName : String) : MetricValue {
        return MetricValue(metricsDb, getKeyspace(), getTableName(),  metricName)
    }

    private fun createMetricCounter(metricName: String) : MetricCounter {
       return MetricCounter(metricsDb, getKeyspace(), getTableName(),metricName)
    }

    // helpers
    fun getRWRatio(): Double {
        val r = readLatency.count.sum()
        val w = writeLatency.count.sum()

        if (w == 0.0) return 0.0
        return r / w
    }

    fun getRWRatioHuman(): String {
        val ratio = getRWRatio()
        if (ratio == 0.0) {
            return "-"
        }
        if (writeLatency.count.sum() == 0.0)
            return "All reads"

        if (ratio < .001)
            return "< .001:1"
        if (ratio > 1000)
            return "> 1000:1"
        return Utils.humanReadableCount(ratio.toLong()) + ":1"
    }

    private fun getRowCacheHitRatio(): Double {
        val r = rowCacheHit.count.sum()
        val w = rowCacheMiss.count.sum()

        if (w == 0.0) return 0.0
        return r / w
    }
    fun getRowCacheRatioHuman(): String {
        val ratio = getRowCacheHitRatio()
        if (ratio == 0.0) {
            return "-"
        }
        if (writeLatency.count.sum() == 0.0)
            return "All reads"

        if (ratio < .001)
            return "< .001:1"
        if (ratio > 1000)
            return "> 1000:1"
        return Utils.humanReadableCount(ratio.toLong()) + ":1"
    }

    fun getReadLatencyP99() : Double {
        return Utils.round((readLatency.p99.max() ?: 0.0) / 1000.0 )
    }

    fun getKsAndTable(): Pair<String, String> {
        val parts = name.split(".")
        return Pair(parts[0], parts[1])
    }

    // little helpers to decrease verbosity
    fun getKeyspace() : String {
        val tmp = getKsAndTable()
        return tmp.first.replace("\"", "")
    }
    fun getTableName() : String {
        return getKsAndTable().second.replace("\"", "")
    }

    fun getOperationsCount(): Int {
        return (readLatency.count.sum() + writeLatency.count.sum()).toInt()
    }


    fun firstClusteringKeyIsTimestamp(): Boolean {
        return clusteringColumns.isNotEmpty()
                && (fields[clusteringColumns.first().first]!!.dataTypeString == "timestamp"
                || fields[clusteringColumns.first().first]!!.dataTypeString == "timeuuid")
    }

    fun partitionKeyIncludesTimestamp(): Boolean {
         return fields.filter { partitionKeys.contains(it.key) }
             .filter { it.value.dataTypeString == "timestamp" || it.value.dataTypeString == "timeuuid" }
             .isNotEmpty()
    }

    /**
     * The next 4 functions will be used initially on the collections section,
     * however, we might want to reuse it elsewhere and it'll be easier to write tests here
     */
    fun hasNonFrozenCollections(): Boolean {
        return fields.values.filter {
            it.dataTypeString.startsWith("map<") ||
                    it.dataTypeString.startsWith("set<") ||
                    it.dataTypeString.startsWith("list<")
        }.count() > 0
    }

    fun hasNonFrozenTypes() : Boolean {
        return fields.values.filter {
            it.dataTypeString.contains(".") && !it.dataTypeString.startsWith("frozen<")
        }.count() > 0
    }

    fun getSets(): Map<String, Field> {
        return fields.filterValues { it.dataTypeString.startsWith("set<") }
    }

    fun getMaps(): Map<String, Field> {
        return fields.filterValues { it.dataTypeString.startsWith("map<") }
    }

    fun getLists(): Map<String, Field> {
        return fields.filterValues { it.dataTypeString.startsWith("list<") }
    }
}