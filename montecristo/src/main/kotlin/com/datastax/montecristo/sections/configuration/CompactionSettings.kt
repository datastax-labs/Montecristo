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
import com.datastax.montecristo.sections.structure.near

class CompactionSettings : DocumentSection {
    /**
    Check the number of concurrent compactors and the maximum compaction throughput.
    If there is less than 16MB/s per compactor, we will advise to either lower the number
    of compactors, or raise the compaction throughput.

    we should really be getting this out of the DB 2555:org.apache.cassandra.db{type=CompactionManager}[]CoreCompactorThreads: 2
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val compactionSection = StringBuilder()

        val compactionThroughput = cluster.getSetting("compaction_throughput_mb_per_sec", ConfigSource.CASS, "16")
        val concurrentCompactors = cluster.getSetting("concurrent_compactors", ConfigSource.CASS, "2")

        compactionSection.append(Utils.formatCassandraYamlSetting(concurrentCompactors))
        compactionSection.append(Utils.formatCassandraYamlSetting(compactionThroughput))

        val concurrentCompactorsValue = concurrentCompactors.getSingleValue().toInt()
        val compactionThroughputValue = compactionThroughput.getSingleValue().toInt()

        if (compactionThroughputValue == 0) {
            recs.near(RecommendationType.CONFIGURATION, unthrottledThroughputRecommencation)
        } else {
            if (compactionThroughputValue / concurrentCompactorsValue < 8) {
                compactionSection.append("\n\nThe throughput per compactor is too low : ${(compactionThroughputValue / concurrentCompactorsValue)}MB/s \n")
                recs.immediate(RecommendationType.CONFIGURATION,throughputPerCompactorRecommendation)
            }
        }

        if (compactionThroughputValue == 16) {
            // This can lead to compaction lagging behind in write heavy/LCS workloads, and the hardware can most probably handle more.
            recs.near(RecommendationType.CONFIGURATION,"""
                We recommend increasing the value of compaction throughput to 64MB/s, unless Cassandra is running on a single spinning disk.
                The default compaction throttling to 16MB/s is usually too low for write-heavy workloads and modern hardware can handle higher values.
            """.trimIndent().replace('\n', ' '))
        }

        if (compactionThroughputValue > 200) {
            recs.near(RecommendationType.CONFIGURATION,"The compaction throughput has been set to $compactionThroughputValue MB/s, which is unusually high. We recommend reviewing the reason that the setting was altered to be this high.")
        }

        args["compactionSettings"] = compactionSection.toString()


        return compileAndExecute("configuration/configuration_compaction.md", args)
    }

    private val throughputPerCompactorRecommendation = "We recommend giving at least 8 MB/s of throughput to each compactor in order to avoid heap pressure due to excessive throttling."
    private val unthrottledThroughputRecommencation = "The compaction is currently un-throttled. We recommend decreasing this value to 64MB/s for clusters using SSD storage and 16MB/s for clusters using spinning disks."

}
