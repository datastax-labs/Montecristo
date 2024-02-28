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

package com.datastax.montecristo.sections.configuration

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class MemtableStorageSettings : DocumentSection {
    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)
        val memtableConfigurationSection = StringBuilder()
        val memtableAllocationType = cluster.getSetting("memtable_allocation_type", ConfigSource.CASS, "heap_buffers")
        val memtableHeapSpace = cluster.getSetting("memtable_heap_space_in_mb", ConfigSource.CASS, "not set")
        val memtableOffHeapSpace = cluster.getSetting("memtable_offheap_space_in_mb", ConfigSource.CASS, "not set")
        val memtableCleanupThreshold = cluster.getSetting("memtable_cleanup_threshold", ConfigSource.CASS, "not set")
        val memtableFlushWriters = cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "not set")

        memtableConfigurationSection.append(Utils.formatCassandraYamlSetting(memtableAllocationType))
        memtableConfigurationSection.append(Utils.formatCassandraYamlSetting(memtableHeapSpace))
        memtableConfigurationSection.append(Utils.formatCassandraYamlSetting(memtableOffHeapSpace))
        memtableConfigurationSection.append(Utils.formatCassandraYamlSetting(memtableCleanupThreshold))
        memtableConfigurationSection.append(Utils.formatCassandraYamlSetting(memtableFlushWriters))

        if (memtableAllocationType.getSingleValue() == "heap_buffers") {
            if (cluster.databaseVersion.supportsOffHeapMemtables()) {
                recs.immediate(RecommendationType.CONFIGURATION,storeMemtablesOffHeapRecommendation)
            }
        }
        args["memtableConfigurationSection"] = memtableConfigurationSection.toString()
        return compileAndExecute("configuration/configuration_memtable_storage.md", args)
    }

    private val storeMemtablesOffHeapRecommendation = "We recommend storing memtables fully off heap to minimize heap pressure and benefit from up to 10% more write throughput. Use `offheap_objects` as `memtable_allocation_type` instead of `heap_buffers`."
}
