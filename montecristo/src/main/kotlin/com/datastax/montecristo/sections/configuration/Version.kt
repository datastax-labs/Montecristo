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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable


class Version : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val advice = getAdvice(cluster.databaseVersion, cluster.isDse)
        advice.forEach {
            recs.add(it)
        }

        val args = super.createDocArgs(cluster)
        args["version"] = cluster.databaseVersion.toString()

        val versionList = cluster.nodes.map { it.databaseVersion.toString() }
        // do we have more than 1 version? this would of be reported as a report generation error, but we should provide details
        // within the report.
        val multipleVersions = MarkdownTable("Version", "Number of Nodes")

        args["multipleVersions"] = if (versionList.toSet().size > 1) {
            versionList.groupingBy { it }.eachCount()
                .map { multipleVersions.addRow().addField(it.key).addField(it.value) }
            recs.immediate(RecommendationType.INFRASTRUCTURE, "We recommend that a single version of ${args["software"]} is used within the cluster.")
            multipleVersions.toString()
        } else {
            ""
        }
        if(!cluster.isDse && cluster.databaseVersion.showCassandra4Upgrade()) {
            args["showC4Upgrade"] = "true"
        }

        return compileAndExecute("configuration/configuration_cassandra_version.md", args)

    }

    /**
     * returns our recommendation for a given version
     */
    fun getAdvice(version: DatabaseVersion, isDSE : Boolean) : List<Recommendation> {
        // unsupported versions
        val recommendationList = mutableListOf<Recommendation>()
        val args = mutableMapOf<String, Any>("version" to version)

        if (isDSE) {
            if (!version.isCommunityMaintained()) {
                recommendationList.add(Recommendation.near(RecommendationType.INFRASTRUCTURE, "The version of DSE currently being used is no longer supported. (https://www.datastax.com/legal/supported-software). We strongly recommend that you upgrade to DSE 5.1, or 6.8."))
            }
        } else {
            val cass4rec = cassandra4Recommendation(version, args)
            if (cass4rec != null) {
                recommendationList.add(cass4rec)
            }
        }
        val patchLatestRec = patchToLatestRecommendation(version)
        if (patchLatestRec != null){
            recommendationList.add(patchLatestRec)
        }

        return recommendationList
    }

    private fun cassandra4Recommendation(
        version: DatabaseVersion,
        args: MutableMap<String, Any>
    ) : Recommendation? {
        var recommendedVersion : DatabaseVersion? = null

        val template = if (version.isCommunityMaintained()) {
            "supported"
        } else {
            "unsupported"
        }
        if (version.showCassandra4Upgrade()) {
            recommendedVersion = DatabaseVersion.latest41()
        }

        if (recommendedVersion != null && template != null) {
            args["recommendedVersion"] = recommendedVersion.toString()
            val file = "configuration/recommendations/version_$template.md"
            val tmp = compileAndExecute(file, args)
            // todo add the short form
            return Recommendation.long(RecommendationType.INFRASTRUCTURE, tmp)
        }
        return null
    }

    private fun patchToLatestRecommendation(
        version: DatabaseVersion,
    ) : Recommendation? {
        if (version != version.latestRelease()) {
            return Recommendation.near(RecommendationType.INFRASTRUCTURE, "We recommend upgrading to the latest patch level, which at the time of writing is ${version.latestRelease()}")
        }
        return null
    }
}
