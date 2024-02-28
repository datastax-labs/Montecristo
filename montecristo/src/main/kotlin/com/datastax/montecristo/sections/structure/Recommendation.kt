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

class Recommendation(val priority: RecommendationPriority,  val type : RecommendationType, val longForm: String, var shortForm: String = "") {
    init {
        if(shortForm.isEmpty()) {
            shortForm = longForm
        }
    }
    companion object {
        fun immediate(type : RecommendationType, longForm: String, shortForm: String = "") : Recommendation {
            return Recommendation(RecommendationPriority.IMMEDIATE, type, longForm, shortForm)
        }
        fun near(type : RecommendationType, longForm: String, shortForm: String = "") : Recommendation {
            return Recommendation(RecommendationPriority.NEAR, type, longForm, shortForm)
        }
        fun long(type : RecommendationType, longForm: String, shortForm: String = "") : Recommendation {
            return Recommendation(RecommendationPriority.LONG, type, longForm, shortForm)
        }

    }
}

/**
 * Convenience extension to create new immediate recommendations
 */
fun MutableList<Recommendation>.immediate(type : RecommendationType, longForm: String, shortForm: String = "") {
    add(Recommendation.immediate(type, longForm,  shortForm))
}
/**
 * Convenience extension to create new near term recommendations
 */
fun MutableList<Recommendation>.near(type : RecommendationType, longForm: String, shortForm: String = "") {
    add(Recommendation.near(type, longForm, shortForm))
}
/**
 * Convenience extension to create new long term recommendations
 */
fun MutableList<Recommendation>.long(type : RecommendationType, longForm: String, shortForm: String = "") {
    add(Recommendation.long(type, longForm, shortForm))
}

