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

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.ByteCountHelperUnits
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.application.JvmSettings
import com.datastax.montecristo.model.os.GCAlgorithm
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class JavaHeapConfiguration : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        val setOfJVMSettings = cluster.nodes.map { it.jvmSettings }.toSet()
        if (setOfJVMSettings.size > 1) {
            // we have more than 1 JVM settings, we should add a recommendation to align them.
            recs.immediate(
                RecommendationType.CONFIGURATION,"""
                    We recommend aligning the JVM settings across the nodes - there are ${setOfJVMSettings.size} different JVM configurations.
                """.trimIndent()
            )
        }

        val jvmSettings = setOfJVMSettings.first()
        args["UsesCMS"] = "false"
        if (jvmSettings.gcAlgorithm == GCAlgorithm.CMS)
            args["UsesCMS"] = "true"

        args["HeapSize"] = ByteCountHelper.humanReadableByteCount(jvmSettings.heapSize, ByteCountHelperUnits.BINARY)
        args["GcAlgorithm"] = jvmSettings.gcAlgorithm
        if(jvmSettings.newGenSize != null) {
            args["NewGenSize"] =  ByteCountHelper.humanReadableByteCount(jvmSettings.newGenSize, ByteCountHelperUnits.BINARY)
        }

        args["GcSettings"] = jvmSettings.gcFlags.joinToString("  \n")

        recommendJvmSettings(jvmSettings, recs, cluster)

        return compileAndExecute("configuration/configuration_java_heap_settings.md", args)

    }


    private fun recommendJvmSettings(parsedJvmSettings: JvmSettings, recs: MutableList<Recommendation>, cluster: Cluster) {
        if (parsedJvmSettings.gcAlgorithm == GCAlgorithm.CMS) {
            // Applying CMS recommendations
            if (parsedJvmSettings.newGenSize < 2_000_000_000 && parsedJvmSettings.heapSize >= 4_000_000_000) {
                // New Gen size is too small
                recs.immediate(RecommendationType.CONFIGURATION,"""
                    We recommend allocating up to 50% of the total heap size to the new gen, especially in read heavy workloads. The current heap's new generation size is smaller than optimal (${ByteCountHelper.humanReadableByteCount(parsedJvmSettings.newGenSize, ByteCountHelperUnits.BINARY)}).
                """.trimIndent()
                )
            }
            println("${cluster.nodes.first().osConfiguration.memInfo.memTotal} total memory")
            if (parsedJvmSettings.heapSize < 8_000_000_000 && ByteCountHelper.parseHumanReadableByteCountToLong("${cluster.nodes.first().osConfiguration.memInfo.memTotal} kB") >= 30_000_000_000) {
                // Heap size is too small
                recs.immediate(RecommendationType.CONFIGURATION,"Your heap size is too small (${ByteCountHelper.humanReadableByteCount(parsedJvmSettings.heapSize, ByteCountHelperUnits.BINARY)})." +
                        " With the available ${ByteCountHelper.humanReadableByteCount(ByteCountHelper.parseHumanReadableByteCountToLong("${cluster.nodes.first().osConfiguration.memInfo.memTotal} kiB"), ByteCountHelperUnits.BINARY)} of RAM," +
                        " we recommend allocating between 12GiB to 16GiB of total heap size.")
            }

        } else {
            // G1GC recommendations
            if (ByteCountHelper.parseHumanReadableByteCountToLong("${cluster.nodes.first().osConfiguration.memInfo.memTotal} kB") < 40_000_000_000) {
                // Available RAM is too small for G1
                recs.immediate(RecommendationType.CONFIGURATION,"G1 ideally requires at least 20GiB of heap space to perform efficiently and you currently don't have enough RAM to use such heap sizes. We recommend using CMS instead which usually performs better than G1 when tuned appropriately.")
            }
            if (ByteCountHelper.parseHumanReadableByteCountToLong("${cluster.nodes.first().osConfiguration.memInfo.memTotal} kB") >= 40_000_000_000 && parsedJvmSettings.heapSize < 20_000_000_000) {
                // Available RAM is too small for G1
                recs.immediate(RecommendationType.CONFIGURATION,"G1 ideally requires at least 20GiB of heap space to perform efficiently. We recommend increasing your heap size to value between 20GiB and 31GiB to maximize its performance.")
            }
            // we should check vs the compressed oops cut off point which is 32765 MB
            if (parsedJvmSettings.heapSize > (32765L * 1024L * 1024L)) {
                recs.immediate(RecommendationType.CONFIGURATION,"G1 heap is at or above 32GiB. Heap sizes of 32GiB and more no longer benefit from compressed ordinary object pointers (oops) which provide the largest number of addressable objects for the smallest heap size. We recommend decreasing your heap size to 31GiB to maximize its performance.")
            }
        }
    }
}
