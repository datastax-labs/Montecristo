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

package com.datastax.montecristo.sections.infrastructure


import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable

class Storage : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        var triggerReadAheadRecommendation = false
        var triggerDeviceFormatRecommendation = false
        var triggerSchedulerRecommendation = false
        var trigger70SpaceRecommendation = false
        var trigger70SpaceWarnCount = 0
        var trigger60SpaceRecommendation = false
        var trigger60SpaceWarnCount = 0
        var trigger50SpaceRecommendation = false
        var trigger50SpaceWarnCount = 0

        val storageOverview = MarkdownTable("Hostname", "Data Device", "Read Ahead", "Scheduler", "Format", "Is Rotational", "Percentage Free")
        val nodesGroupedByDC = cluster.nodes.groupBy {  it.info.dataCenter }.toList()
        nodesGroupedByDC.forEachIndexed { index, dcToNodeList ->
            dcToNodeList.second.sortedBy { it.info.dataCenter + "-" + it.hostname }
                .forEach { node ->
                    val dataDeviceNames = node.storage.storageLocations.dataLocation().split(",") // this can be a CSV.
                    dataDeviceNames.forEach { dataDeviceName ->
                        val dataDevice = node.storage.diskDevices[dataDeviceName]
                        val readAhead = dataDevice?.readAhead ?: ""
                        val scheduler = dataDevice?.scheduler ?: "unknown"
                        val format = node.storage.lsBlk[dataDeviceName]?.fsType ?: "unknown"
                        val isRotational = node.storage.lsBlk[dataDeviceName]?.isRotational
                            ?: false // if we don't know, assume ssd to potentially trigger recommendation
                        val percentageFree = 100.0 - (node.storage.dfSize.usedSpacePercentage(dataDeviceName) ?: 0)
                        // if we do not know the read ahead, assume a high value to trigger the recommendation
                        if (dataDevice?.readAhead ?: 4096 > 16 && !isRotational) {
                            triggerReadAheadRecommendation = true
                        }
                        // any format which is unknown or not xfs will trigger
                        if (format != "xfs") {
                            triggerDeviceFormatRecommendation = true
                        }
                        if (scheduler.contains("kyber") && !scheduler.contains("[kyber]")) {
                            triggerSchedulerRecommendation = true
                        }
                        // if we do not know the space, make the recommendation to be safe
                        when {
                            percentageFree < 30 -> {
                                trigger70SpaceRecommendation = true
                                trigger70SpaceWarnCount++
                            }
                            percentageFree < 40 -> {
                                trigger60SpaceRecommendation = true
                                trigger60SpaceWarnCount++
                            }
                            percentageFree < 50 -> {
                                trigger50SpaceRecommendation = true
                                trigger50SpaceWarnCount++
                            }
                        }

                        storageOverview.addRow()
                            .addField(node.hostname)
                            .addField(dataDeviceName)
                            .addField(readAhead)
                            .addField(scheduler)
                            .addField(format)
                            .addField(isRotational.toString())
                            .addField(percentageFree)
                    }
                }
        }
        args["storage"] = storageOverview.toString()


        if (triggerReadAheadRecommendation) {
            recs.immediate(RecommendationType.INFRASTRUCTURE, "We recommend lowering the read ahead for the data devices to an RA value of 32, which will result in a read ahead of 16 kb.")
        }

        if (triggerDeviceFormatRecommendation) {
            recs.near(RecommendationType.INFRASTRUCTURE, "We recommend using the XFS format for the data devices.")
        }

        if (triggerSchedulerRecommendation) {
            recs.immediate(RecommendationType.INFRASTRUCTURE, "We recommend using the kyber IO scheduler for the devices holding data.")
        }

        val spaceRecs = decideOnSpaceWarning(trigger70SpaceRecommendation, trigger60SpaceRecommendation, trigger50SpaceRecommendation, trigger70SpaceWarnCount, trigger60SpaceWarnCount, trigger50SpaceWarnCount)
        spaceRecs.forEach {
            recs.add(it)
        }

        return compileAndExecute("infrastructure/infrastructure_storage.md", args)
    }

    private fun decideOnSpaceWarning(trigger70SpaceRecommendation : Boolean, trigger60SpaceRecommendation: Boolean, trigger50SpaceRecommendation: Boolean, trigger70SpaceWarnCount : Int, trigger60SpaceWarnCount : Int, trigger50SpaceWarnCount : Int) : List<Recommendation> {
        val recs: MutableList<Recommendation> = mutableListOf()
        val messageCounters : StringBuilder = StringBuilder()
        if (trigger70SpaceWarnCount > 0){
            messageCounters.append("$trigger70SpaceWarnCount data ${"volume reports".plural("volumes report", trigger70SpaceWarnCount)} less than 30% space free. ")
        }
        if (trigger60SpaceWarnCount > 0){
            messageCounters.append("$trigger60SpaceWarnCount data ${"volume reports".plural("volumes report", trigger60SpaceWarnCount)} less than 40% space free. ")
        }
        if (trigger50SpaceWarnCount > 0){
            messageCounters.append("$trigger50SpaceWarnCount data ${"volume reports".plural("volumes report", trigger50SpaceWarnCount)} less than 50% space free. ")
        }
        val startingText = "We recommend having at least 50% of the disk space available on the data volume to ensure that compaction has sufficient space to operate."
        val recommendationPartText = "We recommend reviewing the ${"node".plural("nodes", trigger70SpaceWarnCount + trigger60SpaceWarnCount + trigger50SpaceWarnCount )} and either provision additional disk space or free up space."
        // can only trigger 1 of these
        when {
            trigger70SpaceRecommendation -> {
                recs.immediate(RecommendationType.INFRASTRUCTURE, "$startingText Operating at less than 30% available space is likely to result in compaction failing and nodes becoming unstable. ${messageCounters}$recommendationPartText")
            }
            trigger60SpaceRecommendation -> {
                recs.near(RecommendationType.INFRASTRUCTURE, "$startingText Operating 30%-40% available space risks compaction failing and nodes becoming unstable. ${messageCounters}$recommendationPartText")
            }
            trigger50SpaceRecommendation -> {
                recs.near(RecommendationType.INFRASTRUCTURE, "$startingText ${messageCounters}We recommend further evaluation against the number of tables and their compaction strategies to ensure availability is maintained and capacity planning happens in advance.")
            }
        }
        return recs.toList()
    }
}
