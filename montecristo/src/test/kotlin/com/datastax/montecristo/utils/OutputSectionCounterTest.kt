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

internal class OutputSectionCounterTest {

    // Be aware - writing multiple tests for this causes it to clash, because the tests themselves
    // pick up the singleton, so the 2nd test doesn't start at zero.
    @Test
    fun incrementAndGetTestSingleton() {
        // This is a singleton so I should be able to add to it in different references
        val counter1 = OutputSectionCounter.instance
        val counter2 = OutputSectionCounter.instance

        assertThat(counter1.incrementAndGet()).isEqualTo(1)
        assertThat(counter2.incrementAndGet()).isEqualTo(2)
        assertThat(counter1.incrementAndGet()).isEqualTo(3)
        assertThat(counter2.incrementAndGet()).isEqualTo(4)
    }
}