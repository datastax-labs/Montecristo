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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import io.mockk.mockk
import org.assertj.core.api.Assertions
import org.junit.Test

internal class CollectedArtifactsTest() {
    private val collectedTemplate = "# Artifacts\n" +
            "\n" +
            "The artifacts used for this discovery include:  \n" +
            "\n" +
            "* Cassandra logs\n" +
            "* Cassandra configuration files\n" +
            "* Output from Cassandra nodetool diagnostics\n" +
            "* Various operating system diagnostics\n" +
            "* Schema\n" +
            "* JMX Metrics\n" +
            "\n" +
            "\n"
    @Test
    fun getDocument() {
        val cluster = mockk<Cluster>(relaxed = true)
        val recs: MutableList<Recommendation> = mutableListOf()
        val collected = CollectedArtifacts()
        val searcher = mockk<Searcher>(relaxed = true)
        val response = collected.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        Assertions.assertThat(recs.size).isEqualTo(0)
        Assertions.assertThat(response).isEqualTo(collectedTemplate)
    }
}