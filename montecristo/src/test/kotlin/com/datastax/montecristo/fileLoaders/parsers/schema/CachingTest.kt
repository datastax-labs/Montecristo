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

package com.datastax.montecristo.fileLoaders.parsers.schema

import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable.Companion.noCaching
import com.datastax.montecristo.fileLoaders.parsers.schema.ParsedCreateTable.Companion.parseCachingfromJSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertEquals

class CachingTest {

    @Test
    fun testGetNone() {
        var parsed = noCaching()
        assertEquals(parsed.keys, "NONE")
        assertEquals(parsed.rows, "NONE")

    }
    @Test
    fun testFromJSON() {
        val data = """{"keys":"ALL", "rows_per_partition":"NONE"}"""
        val cache = parseCachingfromJSON(data)
        assertThat(cache.keys).isEqualTo("ALL")
        assertThat(cache.rows).isEqualTo("NONE")
    }
}