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

package com.datastax.montecristo.fileLoaders.parsers.os

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class IfConfigParserTest {

    @Test
    fun testBasicFileParse() {
        val ifConfigTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/ifconfig.txt").reader().readLines()
        val ifConfig = IfConfigParser.parse (ifConfigTxt)
        assertThat(ifConfig.bindings.size).isEqualTo(6)
        assertThat(ifConfig.bindings.containsKey("bond0")).isTrue()
        assertThat(ifConfig.bindings.get("bond0")).isEqualTo("10.102.98.71")

        assertThat(ifConfig.bindings.containsKey("enp179s0f0")).isTrue()
        assertThat(ifConfig.bindings.get("enp179s0f0")).isNull()

        assertThat(ifConfig.bindings.containsKey("enp179s0f1")).isTrue()
        assertThat(ifConfig.bindings.get("enp179s0f1")).isNull()

        assertThat(ifConfig.bindings.containsKey("enp27s0f0")).isTrue()
        assertThat(ifConfig.bindings.get("enp27s0f0")).isEqualTo("10.102.98.7")

        assertThat(ifConfig.bindings.containsKey("enp27s0f1")).isTrue()
        assertThat(ifConfig.bindings.get("enp27s0f1")).isEqualTo("10.102.254.104")

        assertThat(ifConfig.bindings.containsKey("lo")).isTrue()
        assertThat(ifConfig.bindings.get("lo")).isEqualTo("127.0.0.1")
    }

}