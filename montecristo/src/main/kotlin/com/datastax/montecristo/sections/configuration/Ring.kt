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
import com.datastax.montecristo.model.nodetool.Ring
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Partitioner
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.long
import com.datastax.montecristo.utils.MarkdownTable
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

class Ring : DocumentSection {

    private val ringRebalanceRecommendation =  "The token range assignment is unbalanced in the following data centers : {dc_name}. Further work will need to be done to document the remediation process in a runbook. The process must be done by creating a new data center and shifting traffic to it."

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        // grab an arbitrary node which will have the ring within it
        val nodeRingTokens = cluster.nodes.toList().first().ring
        val partitioner = if (cluster.nodes.first().cassandraYaml.partitioner == Partitioner.RANDOM.yamlSetting) { Partitioner.RANDOM } else { Partitioner.MURMUR }
        val tokenResults =  calculateTokenPercentage(partitioner, nodeRingTokens)

        val balancedResults = isTokenRingBalanced(tokenResults, executionProfile.limits.tokenOwnershipPercentageImbalanceThreshold)
        val unbalancedDcCount = balancedResults.count { d -> !d.value }
        if (unbalancedDcCount > 0) {
            val dcNameList = balancedResults.filter { d -> !d.value }.toList().joinToString { it.first }
            recs.long(RecommendationType.CONFIGURATION,ringRebalanceRecommendation.replace("{dc_name}", dcNameList))
        }
        // need to create a formatted output of the results.
        val md = MarkdownTable("DC", "Node", "Percentage Owned")
        for (dc in tokenResults) {
            dc.value.toList().sortedByDescending { (_, value) -> value }.toMap().forEach { n ->
                md.addRow().addField(dc.key)
                        .addField(n.key)
                        .addField(String.format(Locale.ENGLISH, "%.2f",n.value))
            }
            md.addRow().addBlankRow(3)
        }
        args["ring"] = md.toString()

        return compileAndExecute("configuration/configuration_ring.md", args)
    }


    internal fun calculateTokenPercentage(partitioner : Partitioner, ringTokens: List<Ring>): HashMap<String, HashMap<String, Double>> {

        // the first node of the ring has a wrap around from the last token range, so it goes from a very high positive to a very low negative
        // which gives an exception case in the normal maths.
        val allDCPercentageAllocations = hashMapOf<String, HashMap<String, Double>>()

        for (dc in ringTokens) {
            val datacenterName = dc.datacenter
            allDCPercentageAllocations.putIfAbsent(datacenterName, hashMapOf())
            for (node in dc.nodes) {
                val nodeName = node.key
                var tokenRangeSum = BigInteger.ZERO
                for (nodeTokenRange in node.value) {
                    if (nodeTokenRange.startRange > nodeTokenRange.endRange) {
                        // this is the special first case, would be nicer in a map
                            // depending on the partitioner, the notional max value is different (2^63 vs 2^127)
                        if (partitioner == Partitioner.RANDOM) {
                            tokenRangeSum +=  BigInteger.valueOf(2).pow(127).minus(BigInteger.ONE).minus(nodeTokenRange.startRange )

                        } else {
                            // calculate the gap from the start to the positive max
                            tokenRangeSum += BigInteger.valueOf(Long.MAX_VALUE) - (nodeTokenRange.startRange)
                            // calculate the gap from negative max to the end of range
                            tokenRangeSum += (nodeTokenRange.endRange) - BigInteger.valueOf(Long.MIN_VALUE)
                        }
                    } else {
                        tokenRangeSum += (nodeTokenRange.endRange ) - (nodeTokenRange.startRange )
                    }
                }
                val percentOfTokenRange =  if (partitioner == Partitioner.RANDOM) {
                    (tokenRangeSum.toBigDecimal().divide(BigInteger.valueOf(2).pow(127).toBigDecimal()) * BigDecimal.valueOf(100.0))
                } else {
                    (tokenRangeSum.toBigDecimal().divide(BigInteger.valueOf(2).pow(64).toBigDecimal()) * BigDecimal.valueOf(100.0))
                }
                allDCPercentageAllocations[datacenterName]?.put(nodeName, percentOfTokenRange.toDouble())
            }
        }
        return allDCPercentageAllocations
    }

    internal fun isTokenRingBalanced(nodeRingTokens: HashMap<String, HashMap<String, Double>>, maximumPercentageDifference: Double): HashMap<String, Boolean> {
        // given map of <DC<Node,Percentage>> - do we consider the token ranges balance in each DC
        // this gets subjective in terms of what is unbalanced? for simplicity in the first instance we will consider
        // the min to max - if the minimum percentage is lower by more than 25% of the maximum, then its not balanced.
        // e.g. If the max percentage is 10% on 1 node, we are willing to tolerate as low as 7.5%
        // For flexibility, this tolerance is passed in as a percentage (for testing), and is a constant at the top if we need to change it.
        val results = hashMapOf<String, Boolean>()
        // process each DC individually and add to the results
        for (datacenter in nodeRingTokens) {
            val min = Collections.min(datacenter.value.values)
            val max = Collections.max(datacenter.value.values)
            // is the minimum above the max reduced by the percentage tolerance
            results[datacenter.key] = max * (1.0 - maximumPercentageDifference) < min
        }
        return results
    }
}