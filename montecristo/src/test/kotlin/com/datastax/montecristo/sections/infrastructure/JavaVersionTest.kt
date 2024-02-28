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
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class JavaVersionTest {

    @Test
    fun getDocumentMinorUpgrade() {
        val versionLine = "openjdk version \"1.8.0_181\"\nOpenJDK Runtime Environment (build 1.8.0_181-b13)\nOpenJDK 64-Bit Server VM (build 25.181-b13, mixed mode)"
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.first().osConfiguration.javaVersion } returns versionLine
        val javaVersionSection = JavaVersion()
        val recs:MutableList<Recommendation> = mutableListOf()

        javaVersionSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // check the template, and the recommendations
        assertThat(recs.first().priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs.first().longForm).isEqualTo("The Java 8 version in use (1.8.0_181) is outdated and should be upgraded for stability and performance improvements. The latest OpenJDK 8 release should be used instead.")
    }

    @Test
    fun getDocumentMajorUpgrade() {
        val versionLine = "1.7.0_0"
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.first().osConfiguration.javaVersion } returns versionLine
        val javaVersionSection = JavaVersion()
        val recs:MutableList<Recommendation> = mutableListOf()

        javaVersionSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // check the template, and the recommendations
        assertThat(recs.get(0).priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs.get(0).longForm).isEqualTo("Upgrade to Java 8.")
      }

    @Test
    fun getDocumentNoUpgrade() {
        val versionLine = "openjdk version \"1.8.0_262\"\nOpenJDK Runtime Environment (build 1.8.0_262-b13)\nOpenJDK 64-Bit Server VM (build 25.181-b13, mixed mode)"
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.first().osConfiguration.javaVersion } returns versionLine
        val javaVersionSection = JavaVersion()
        val recs:MutableList<Recommendation> = mutableListOf()

        javaVersionSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // check the template, and the recommendations
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getJavaVersion8() {
        val versionLine = "openjdk version \"1.8.0_181\"\nOpenJDK Runtime Environment (build 1.8.0_181)\nOpenJDK 64-Bit Server VM (build 25.181-b13, mixed mode)"
        val javaVersionSection = JavaVersion()
        assertThat(javaVersionSection.getJavaVersion(versionLine)).isEqualTo("1.8.0_181")
    }

    @Test
    fun getDocumentNullVersion() {
        val versionLine = ""
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes.first().osConfiguration.javaVersion } returns versionLine
        val javaVersionSection = JavaVersion()
        val recs:MutableList<Recommendation> = mutableListOf()

        javaVersionSection.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // check the template, and the recommendations
        assertThat(recs.size).isEqualTo(0)
    }
}