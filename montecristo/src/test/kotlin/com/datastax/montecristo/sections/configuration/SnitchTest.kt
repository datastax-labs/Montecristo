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
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class SnitchTest {

    @Test
    fun getDocumentGossipFilePropertySnitch() {

        val configSetting = ConfigurationSetting("endpoint_snitch", mapOf(Pair("node1", ConfigValue(true, "SimpleSnitch","GossipingPropertyFileSnitch")), Pair("node2", ConfigValue(true, "SimpleSnitch","GossipingPropertyFileSnitch"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("endpoint_snitch", ConfigSource.CASS ,"SimpleSnitch") } returns configSetting

        val snitch = Snitch()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = snitch.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("endpoint_snitch: GossipingPropertyFileSnitch") // template is very simple, using contains
    }

    @Test
    fun getDocumentSimpleSnitch() {

        val configSetting = ConfigurationSetting("endpoint_snitch", mapOf(Pair("node1", ConfigValue(true, "SimpleSnitch","SimpleSnitch")), Pair("node2", ConfigValue(true, "SimpleSnitch","SimpleSnitch"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("endpoint_snitch",  ConfigSource.CASS ,"SimpleSnitch") } returns configSetting


        val snitch = Snitch()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = snitch.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend avoiding using the SimpleSnitch or the PropertyFileSnitch and use the GossipingPropertyFileSnitch instead.")
        assertThat(template).contains("endpoint_snitch: SimpleSnitch")  // template is very simple, using contains
    }

    @Test
    fun getDocumentDSESimpleSnitch() {

        val configSetting = ConfigurationSetting("endpoint_snitch", mapOf(Pair("node1", ConfigValue(true, "SimpleSnitch","DseSimpleSnitch")), Pair("node2", ConfigValue(true, "SimpleSnitch","DseSimpleSnitch"))))
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.getSetting("endpoint_snitch",  ConfigSource.CASS ,"SimpleSnitch") } returns configSetting


        val snitch = Snitch()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = snitch.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend avoiding using the DseSimpleSnitch and use the GossipingPropertyFileSnitch instead.")
        assertThat(template).contains("endpoint_snitch: DseSimpleSnitch")  // template is very simple, using contains
    }
}