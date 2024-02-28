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
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near

class Seeds : DocumentSection {
    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)
        val seedSection = StringBuilder()

        // seedProvider is the full yaml entry, but its awkward to manipulate, needed for the raw output in the doc.
        val seedProvider = cluster.getSetting("seed_provider", ConfigSource.CASS).values
        // seed list is coming from the helper, list<String> and is easier to process / work with for the logic
        val seedList = cluster.nodes.map { it.cassandraYaml.seeds }.toList()

        if (hasConsistentSeeds(seedList)) {
            // all the same, use the first one
            val seeds: String = seedList.first()
            seedSection.append("Seed list is configured as follows: \n\n")
            seedSection.append("```\n")
            seedSection.append("- seeds: $seeds \n")
            seedSection.append("```\n\n")
            if (!hasEnoughSeeds(cluster.nodes, seeds)) {
                recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.CONFIGURATION, notEnoughSeedsRecommendation))
            }
            if (!allSeedsExist(cluster.nodes, seeds)) {
                recs.add(Recommendation(RecommendationPriority.NEAR,RecommendationType.CONFIGURATION, seedNotFoundRecommendation))
            }
        } else {
            seedSection.append("Seed list is inconsistent across the cluster: \n\n")
            seedSection.append("```\n")
            seedSection.append(Utils.displayInconsistentConfig(seedProvider.entries))
            seedSection.append("\n```\n")
            recs.near(RecommendationType.CONFIGURATION,"We recommend using the same seed nodes across the cluster. Our advice is to use 3 nodes from each datacenter as seeds.")
        }

        args["seedConfiguration"] = seedSection.toString()
        return compileAndExecute("configuration/configuration_seeds.md", args)
    }


    internal fun hasEnoughSeeds(nodes: List<Node>, seeds: String): Boolean {
        for (datacenter in nodes.map { it.info.dataCenter }.toList()) {
            var dcSeedCount = 0
            // filter the nodes to the DC we are dealing with, then filter it again to be where the seeds list contains the node label or listen address
            // if we don't see 3, then issue the recommendation for 3 seeds per DC
            for (node in nodes.filter { it.info.dataCenter == datacenter }) {
                // the seed could be in as the label or the IP, need to check both
                if (seeds.split(",").any { it.trim() == node.hostname || it.trim() == node.listenAddress }) {
                    dcSeedCount++
                }
            }
            if (dcSeedCount != 3)
                return false
        }
        // if we have not broken out of the loop with a false, then all the DC's seeds must be good.
        return true
    }

    private fun allSeedsExist(nodes: List<Node>, seeds: String): Boolean {
        val seedsList = seeds.split(",")
        val nodeListenAddressList = nodes.map { n -> n.listenAddress }

        // filter the seed list to those not contained in the node listen address list
        // if there are none left, then they all exist, if any are left, the equality fails and
        // it means there are seeds not in the list.
        return seedsList.none { !nodeListenAddressList.contains(it.trim()) }
    }

    internal fun hasConsistentSeeds(seedsList: List<String>): Boolean {
        // the seed consistency check needs to account for seeds being in a different order within the string
        // e.g. the seed list of a,b,c is equivalent to c,b,a - and shouldn't trigger a recommendation.
        // simplest check is to split each string csv, order them, re-combine back to csv - then with a consistent order
        // of values - checking they match is simple using the set operation
        return seedsList.map { it.split(",").map { seedList -> seedList.trim() }.sorted() }.map { it.joinToString(",") }.toSet().size == 1
    }

    private val notEnoughSeedsRecommendation =
        "We recommend putting at least three nodes of each datacenter in the seeds list."
    private val seedNotFoundRecommendation =
        "We recommend checking the seed list to ensure that all of the seed IP addresses do belong to the cluster. A seed was found within the seed list which we could not confirm as being a member of the cluster."
}
