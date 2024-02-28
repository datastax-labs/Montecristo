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

package com.datastax.montecristo.sections.structure

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.profiles.ExecutionProfile
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SummaryTest {

    private val summaryTemplate = "# Summary of findings\n" +
            "\n" +
            "## Report Generation Errors\n" +
            "There were no errors loading the data collected\n" +
            "\n" +
            "## Immediate issues\n" +
            "\n" +
            "* Others\n" +
            "  * short form - immediate rec\n" +
            "\n" +
            "## Near term changes \n" +
            "\n" +
            "* Others\n" +
            "  * short form - near rec\n" +
            "\n" +
            "\n" +
            "## Long term changes \n" +
            "\n" +
            "* Others\n" +
            "  * short form - long rec\n"

    @Test
    fun getDocument() {

        val immediate = mutableListOf<Recommendation>()
        immediate.add(Recommendation(RecommendationPriority.IMMEDIATE, RecommendationType.UNCLASSIFIED,"Long form - immediated rec", "short form - immediate rec"))
        val near = mutableListOf<Recommendation>()
        near.add(Recommendation(RecommendationPriority.NEAR,RecommendationType.UNCLASSIFIED, "Long form - near rec", "short form - near rec"))
        val longterm = mutableListOf<Recommendation>()
        longterm.add(Recommendation(RecommendationPriority.LONG, RecommendationType.UNCLASSIFIED,"Long form - long rec", "short form - long rec"))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)

        val summary = Summary(immediate, near, longterm)
        val template = summary.getDocument(cluster, searcher, mutableListOf(), ExecutionProfile.default())
        assertThat(template).contains("**INSTRUCTIONS FOR DELIVERY**")
        assertThat(template).contains(summaryTemplate)
    }

    @Test
    fun getDocumentWithLoadError() {

        val immediate = mutableListOf<Recommendation>()
        immediate.add(Recommendation(RecommendationPriority.IMMEDIATE, RecommendationType.UNCLASSIFIED,"Long form - immediated rec", "short form - immediate rec"))
        val near = mutableListOf<Recommendation>()
        near.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.UNCLASSIFIED,"Long form - near rec", "short form - near rec"))
        val longterm = mutableListOf<Recommendation>()
        longterm.add(Recommendation(RecommendationPriority.LONG, RecommendationType.UNCLASSIFIED,"Long form - long rec", "short form - long rec"))

        val loadErrorList = mutableListOf(LoadError("node1", "an error"))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.loadErrors } returns loadErrorList

        val summary = Summary(immediate, near, longterm)
        val template = summary.getDocument(cluster, searcher, mutableListOf(),ExecutionProfile.default())
        assertThat(template).contains("1|node1|an error")
    }

    @Test
    fun getDocumentWithLoadErrorsGrouped() {

        val immediate = mutableListOf<Recommendation>()
        immediate.add(Recommendation(RecommendationPriority.IMMEDIATE, RecommendationType.UNCLASSIFIED,"Long form - immediated rec", "short form - immediate rec"))
        val near = mutableListOf<Recommendation>()
        near.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.UNCLASSIFIED,"Long form - near rec", "short form - near rec"))
        val longterm = mutableListOf<Recommendation>()
        longterm.add(Recommendation(RecommendationPriority.LONG, RecommendationType.UNCLASSIFIED,"Long form - long rec", "short form - long rec"))

        val loadErrorList = mutableListOf(LoadError("node1", "an error"), LoadError("node2", "an error"), LoadError("node3", "another error"))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.loadErrors } returns loadErrorList

        val summary = Summary(immediate, near, longterm)
        val template = summary.getDocument(cluster, searcher, mutableListOf(),ExecutionProfile.default())
        assertThat(template).contains("2|node1,node2|an error")
        assertThat(template).contains("1|node3|another error")
    }
}