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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class RecommendationTest {

    @Test
    fun createImmediateNoShortForm() {
        val recommendationText = "test long form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED, recommendationText)
        assertThat(recommendation.longForm).isEqualTo(recommendationText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationText)
    }

    @Test
    fun createImmediate() {
        val recommendationLongText = "test long form"
        val recommendationShortText = "test long form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED, recommendationLongText,recommendationShortText)
        assertThat(recommendation.longForm).isEqualTo(recommendationLongText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationShortText)
    }

    @Test
    fun createNearNoShortForm() {
        val recommendationText = "test near form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED, recommendationText)
        assertThat(recommendation.longForm).isEqualTo(recommendationText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationText)
    }

    @Test
    fun createNear() {
        val recommendationLongText = "test long form"
        val recommendationShortText = "test short form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED, recommendationLongText, recommendationShortText)
        assertThat(recommendation.longForm).isEqualTo(recommendationLongText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationShortText)
    }


    @Test
    fun createLongNoShortForm() {
        val recommendationText = "test long form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED, recommendationText)
        assertThat(recommendation.longForm).isEqualTo(recommendationText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationText)
    }

    @Test
    fun createLong() {
        val recommendationLongText = "test long form"
        val recommendationShortText = "test short form"
        val recommendation = Recommendation.immediate(RecommendationType.UNCLASSIFIED,recommendationLongText, recommendationShortText)
        assertThat(recommendation.longForm).isEqualTo(recommendationLongText)
        assertThat(recommendation.shortForm).isEqualTo(recommendationShortText)
    }

    @Test
    fun listRecommendationImmediate() {
        val myList = mutableListOf<Recommendation>()
        myList.immediate(RecommendationType.UNCLASSIFIED, "long form")
        assertThat(myList.size).isEqualTo(1)
        assertThat(myList[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(myList[0].type).isEqualTo(RecommendationType.UNCLASSIFIED)
        assertThat(myList[0].longForm).isEqualTo("long form")
    }


    @Test
    fun listRecommendationNear() {
        val myList = mutableListOf<Recommendation>()
        myList.near(RecommendationType.UNCLASSIFIED, "long form")
        assertThat(myList.size).isEqualTo(1)
        assertThat(myList[0].priority).isEqualTo(RecommendationPriority.NEAR)
        assertThat(myList[0].type).isEqualTo(RecommendationType.UNCLASSIFIED)
        assertThat(myList[0].longForm).isEqualTo("long form")
    }


    @Test
    fun listRecommendationLong() {
        val myList = mutableListOf<Recommendation>()
        myList.long(RecommendationType.UNCLASSIFIED,"long form")
        assertThat(myList.size).isEqualTo(1)
        assertThat(myList[0].priority).isEqualTo(RecommendationPriority.LONG)
        assertThat(myList[0].type).isEqualTo(RecommendationType.UNCLASSIFIED)
        assertThat(myList[0].longForm).isEqualTo("long form")
    }

}