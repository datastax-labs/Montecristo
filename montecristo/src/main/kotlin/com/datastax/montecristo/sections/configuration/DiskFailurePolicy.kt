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
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near


class DiskFailurePolicy : DocumentSection {

    private val diskPolicyParts = StringBuilder()
    private val commitPolicyParts = StringBuilder()

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val diskFailureSetting = cluster.getSetting("disk_failure_policy", ConfigSource.CASS, "stop")
        val commitFailureSetting = cluster.getSetting("commit_failure_policy", ConfigSource.CASS, "stop")

        if (diskFailureSetting.isConsistent()) {
            diskPolicyParts.append("```\n")
            diskPolicyParts.append("disk_failure_policy: ${diskFailureSetting.getSingleValue()} \n")
            diskPolicyParts.append("```\n\n")
        } else {
            diskPolicyParts.append("Settings are inconsistent across the cluster\n\n")
            diskPolicyParts.append("```\n")
            diskFailureSetting.values.forEach { entry -> diskPolicyParts.append(entry.key + " = " + entry.value.getConfigValue() + "\n") }
            diskPolicyParts.append("```\n\n")
        }

        if (!diskFailureSetting.isConsistent() || diskFailureSetting.getSingleValue() != "die") {
            recs.near(RecommendationType.CONFIGURATION,diskFailureRecommendation)
        }

        if (commitFailureSetting.isConsistent()) {
            commitPolicyParts.append("```\n")
            commitPolicyParts.append("commit_failure_policy: ${commitFailureSetting.getSingleValue()} \n")
            commitPolicyParts.append("```\n\n")
        } else {
            commitPolicyParts.append("Settings are inconsistent across the cluster\n\n")
            commitPolicyParts.append("```\n")
            commitFailureSetting.values.forEach { entry -> commitPolicyParts.append(entry.key + " = " + entry.value + "\n") }
            commitPolicyParts.append("```\n\n")
        }

        if (!commitFailureSetting.isConsistent() || commitFailureSetting.getSingleValue() != "die") {
            recs.near(RecommendationType.CONFIGURATION,commitFailureRecommendation)
        }

        args["diskFailurePolicySetting"] = diskPolicyParts.toString()
        args["commitFailurePolicySetting"] = commitPolicyParts.toString()
        return compileAndExecute("configuration/configuration_disk_policy.md", args)
    }


    private val diskFailureRecommendation = "We recommend setting the `disk_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages."
    private val commitFailureRecommendation = "We recommend setting the `commit_failure_policy` in the cassandra.yaml to a value of **stop** if the Cassandra service is configured to auto-restart. Otherwise, set the value to **die** to improve outage detection for ops teams and prevent hidden outages."

}
