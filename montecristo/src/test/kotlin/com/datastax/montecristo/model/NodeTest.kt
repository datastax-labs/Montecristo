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

package com.datastax.montecristo.model

import com.datastax.montecristo.fileLoaders.Artifacts

class NodeTest {

    lateinit var node : Node
    lateinit var artifacts: Artifacts
    // TODO - we should be able to resurrect these now we are moving to IoC
    // Tests aren't passing anymore due to missing resources.
    /* @Before
    fun setUp() {
        val artifacts = Artifacts(this.javaClass.getResource("/sample-collector").file)
        val location = File(this.javaClass.getResource("/sample-collector").file, "extracted/node1_artifacts_2018_06_28_2128_1530221315")
        node = Node.create(artifacts, location)
    }

    @Test
    fun getHostName() {
        assertThat(node.hostname).isEqualTo("node1.lxd")
        assertThat(node.label).contains("node1.lxd")
    }

    @Test
    fun getMemInfo() {
        assertThat(node.configuration.meminfo.memTotal).isEqualTo(32824940)
    } */
}