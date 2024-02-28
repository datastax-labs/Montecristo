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

import com.datastax.montecristo.helpers.ByteCountHelper
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class StatusParserTest {

    @Test
    fun testStatusParse() {
        val content = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/status/nodetool_status.txt").reader().readLines()
        val status = StatusParser.parse (content)

        assertThat(status.nodes.count()).isEqualTo(5)
        assertThat(status.nodes[0].address).isEqualTo("10.220.40.157")
        assertThat(status.nodes[0].datacenter).isEqualTo("DC1")
        assertThat(status.nodes[0].status).isEqualTo("UN")
        assertThat(status.nodes[0].load).isEqualTo(ByteCountHelper.parseHumanReadableByteCountToLong("7.59 TB"))
        assertThat(status.nodes[0].tokens).isEqualTo(256)
        assertThat(status.nodes[0].ownership).isEqualTo("?")
        assertThat(status.nodes[0].hostId).isEqualTo("71d763ea-f0e6-4ac4-b78c-ba70a41decb3")
        assertThat(status.nodes[0].rack).isEqualTo("22")

        assertThat(status.nodes[1].address).isEqualTo("10.220.40.26")
        assertThat(status.nodes[1].datacenter).isEqualTo("DC1")
        assertThat(status.nodes[1].status).isEqualTo("UN")
        assertThat(status.nodes[1].load).isEqualTo(ByteCountHelper.parseHumanReadableByteCountToLong("8.46 GB"))
        assertThat(status.nodes[1].tokens).isEqualTo(256)
        assertThat(status.nodes[1].ownership).isEqualTo("?")
        assertThat(status.nodes[1].hostId).isEqualTo("f8cfe2cd-e8b5-43d7-8b3b-36187a267ae3")
        assertThat(status.nodes[1].rack).isEqualTo("11")

        assertThat(status.nodes[2].address).isEqualTo("10.220.40.135")
        assertThat(status.nodes[2].datacenter).isEqualTo("DC1")
        assertThat(status.nodes[2].status).isEqualTo("UN")
        assertThat(status.nodes[2].load).isEqualTo(ByteCountHelper.parseHumanReadableByteCountToLong("7.08 MB"))
        assertThat(status.nodes[2].tokens).isEqualTo(256)
        assertThat(status.nodes[2].ownership).isEqualTo("?")
        assertThat(status.nodes[2].hostId).isEqualTo("06eda88f-274c-4c6b-9ef7-887727cf40a0")
        assertThat(status.nodes[2].rack).isEqualTo("21")

        assertThat(status.nodes[3].address).isEqualTo("10.220.40.80")
        assertThat(status.nodes[3].datacenter).isEqualTo("DC1")
        assertThat(status.nodes[3].status).isEqualTo("UN")
        assertThat(status.nodes[3].load).isEqualTo(ByteCountHelper.parseHumanReadableByteCountToLong("7.42 KB"))
        assertThat(status.nodes[3].tokens).isEqualTo(256)
        assertThat(status.nodes[3].ownership).isEqualTo("?")
        assertThat(status.nodes[3].hostId).isEqualTo("534dbb94-c3ed-4781-ac4d-586e1c2e910e")
        assertThat(status.nodes[3].rack).isEqualTo("13")

        assertThat(status.nodes[4].address).isEqualTo("10.220.40.210")
        assertThat(status.nodes[4].datacenter).isEqualTo("DC1")
        assertThat(status.nodes[4].status).isEqualTo("UN")
        assertThat(status.nodes[4].load).isEqualTo(ByteCountHelper.parseHumanReadableByteCountToLong("10.5 PB"))
        assertThat(status.nodes[4].tokens).isEqualTo(256)
        assertThat(status.nodes[4].ownership).isEqualTo("?")
        assertThat(status.nodes[4].hostId).isEqualTo("689c1394-329a-4b54-85fb-2ee25117cd29")
        assertThat(status.nodes[4].rack).isEqualTo("23")
    }
}