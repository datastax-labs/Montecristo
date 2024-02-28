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

package com.datastax.montecristo.utils

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class HumanCountTest {

    @Test
    fun compareTo() {
        val h1 = HumanCount(1000)
        val h2 = HumanCount(1000)

        assertThat(h1).isEqualTo(h2)
    }
}