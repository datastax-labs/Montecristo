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
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.RecommendationType

class Snitch : DocumentSection {

    private val customParts = StringBuilder()

    /**
    Check which snitch is currently used.
    Issue a recommendation if it is either SimpleSnitch or PropertyFileSnitch
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val snitch = cluster.getSetting("endpoint_snitch", ConfigSource.CASS, "SimpleSnitch")

        if (snitch.values.isNotEmpty()) {
            customParts.append(Utils.formatCassandraYamlSetting(snitch))
            val snitchValues = snitch.getDistinctValues()
            if (snitchValues.any { it.value == "SimpleSnitch" || it.value == "PropertyFileSnitch" }) {
                recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.CONFIGURATION, gossipingSnitchRecommendation))
            } else if (snitchValues.any { it.value == "DseSimpleSnitch"}) {
                recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.CONFIGURATION, gossipingSnitchRecommendationDse))
            }

            args["snitch"] = customParts.toString()
        } else {
            args["snitch"] = "No snitch settings found"
        }
        return compileAndExecute("configuration/configuration_snitch.md", args)
    }


    private val gossipingSnitchRecommendation = "We recommend avoiding using the SimpleSnitch or the PropertyFileSnitch and use the GossipingPropertyFileSnitch instead."
    private val gossipingSnitchRecommendationDse = "We recommend avoiding using the DseSimpleSnitch and use the GossipingPropertyFileSnitch instead."

}
