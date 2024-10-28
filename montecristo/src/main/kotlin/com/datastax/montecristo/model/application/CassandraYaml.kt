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

package com.datastax.montecristo.model.application

import com.fasterxml.jackson.databind.JsonNode

data class CassandraYaml(val data : JsonNode)  : YamlConfig(data) {

    // helpers for specific properties
    val clusterName get() = getValueFromPath("cluster_name", "")
    val vNodes get() = getValueFromPath("num_tokens", "1").toInt()
    val seeds get() = getValueFromPath("seed_provider.parameters.seeds", "unknown")
    val listenAddress get() = getValueFromPath("listen_address","")
    val broadcastAddress get() = getValueFromPath("broadcast_address","")
    val compactionThroughput get() = get("compaction_throughput_mb_per_sec")
    val concurrentCompactors get() = get("concurrent_compactors")
    val partitioner get() = get("partitioner")
    val memtableAllocationType get() = get("memtable_allocation_type")
    val authorizer get() = get("authorizer")
    val rowCacheSizeInMB get() = (
            get("row_cache_size", "0").replace("\\D+".toRegex(), "")
            ).toInt()
     fun isRowCacheEnabled(): Boolean {
        return (rowCacheSizeInMB.toString().toInt() > 0)
    }
}

