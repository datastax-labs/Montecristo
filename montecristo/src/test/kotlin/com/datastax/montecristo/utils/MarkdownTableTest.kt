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

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class MarkdownTableTest {
    @Test
    fun testRenderNoData() {
        val message = "No data found"
        val m = MarkdownTable("First", "Second").orMessage(message).toString()
        assertEquals(message, m)

    }

    @Test
    fun testSomeData() {
        val message = "No data found"
        val t = MarkdownTable("First", "Second").orMessage(message)
        t.addRow()
                .addField("test")
                .addField("field")

        val m = t.toString()


        assertNotEquals(message, m)
        assertTrue { m.contains("test|field") }
    }
}