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

package com.datastax.montecristo.fileLoaders.parsers.nodetool

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class InfoParserTest {

    @Test
    fun testEmptyNodetoolInfoTxtFile() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/info/nodetool_info_empty.txt").reader().readLines()
        val nodeInfo = InfoParser.parse(content)

        assertThat(nodeInfo.dataCenter).isEqualTo("UNKNOWN")
        assertThat(nodeInfo.rack).isEqualTo("UNKNOWN")
        assertThat(nodeInfo.loadInBytes).isEqualTo((-1).toLong())
        assertThat(nodeInfo.upTimeInSeconds).isEqualTo(null)
    }

    @Test
    fun testCompleteNodetoolInfoTxtFile() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/info/nodetool_info_complete.txt").reader().readLines()
        val nodeInfo = InfoParser.parse(content)

        assertThat(nodeInfo.dataCenter).isEqualTo("datacenter1")
        assertThat(nodeInfo.rack).isEqualTo("rack1")
        assertThat(nodeInfo.loadInBytes).isEqualTo(2220000000000)
        assertThat(nodeInfo.upTimeInSeconds).isEqualTo(3688807.toLong())
    }
}