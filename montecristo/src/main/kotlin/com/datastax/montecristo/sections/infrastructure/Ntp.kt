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


class Ntp : DocumentSection {

    private val customParts = StringBuilder()

    /**
    Ntp is examined:
    - there should be ntpd in ps output
    - in the ntpq the servers should be different, the poll should be the same, plus we grab jitter an offset
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val nodesWithoutNtp = cluster.nodes.filter {
            !it.osConfiguration.psAux.data.joinToString("\n").contains("ntpd")
        }

        // examine ps output
        val ntpdStatus = StringBuilder()

        if (nodesWithoutNtp.isEmpty()) {
            ntpdStatus.append("The ntp process is running everywhere.\n")
        } else {
            ntpdStatus.append("The ntp process was NOT running:\n")
            nodesWithoutNtp
                    .map { it.hostname }
                    .sorted()
                    .forEach { ntpdStatus.append("$it\n") }
            recs.immediate(RecommendationType.INFRASTRUCTURE,"The NTP daemon (ntpd) was not detected running on all nodes in the cluster. Out of sync clocks in a Cassandra cluster can lead to inconsistencies that can be hard to diagnose. It is critical having ntpd running on all nodes in the cluster with monitoring and alerting of clock drift. We recommend verifying this.")
        }

        val recentServerNames = StringBuilder()
        recentServerNames.append("Recent NTP server names used are:\n")

        val offsetAndJitter = StringBuilder()
        offsetAndJitter.append("Poll\tOffset\tJitter\tNode\n")

        cluster.nodes.forEach { node ->
            recentServerNames.append("${node.hostname} ${node.osConfiguration.ntp.server}\n")

            val poll = node.osConfiguration.ntp.poll ?: "UNKNOWN_POLL"
            val offset = node.osConfiguration.ntp.offset ?: "UNKNOWN_OFFSET"
            val jitter = node.osConfiguration.ntp.jitter ?: "UNKNOWN_JITTER"
            offsetAndJitter.append("${poll}\t${offset}\t${jitter}\t${node.hostname}\n")
        }
        // build the report
        customParts.append(ntpdStatus)
        customParts.append("\n")
        customParts.append(recentServerNames)
        customParts.append("\n")
        customParts.append(offsetAndJitter)
        customParts.append("\n")
        args["ntp"] = customParts.toString()

        if (cluster.nodes.map {  it.osConfiguration.ntp.poll }.toSet().size > 1) {
            recs.near(RecommendationType.INFRASTRUCTURE,"We recommend using the same polling interval on every node.")
        }
        return compileAndExecute("infrastructure/infrastructure_ntp.md", args)
    }
}