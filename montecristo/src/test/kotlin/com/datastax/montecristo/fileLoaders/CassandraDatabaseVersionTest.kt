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

package com.datastax.montecristo.fileLoaders

import com.datastax.montecristo.model.versions.DatabaseVersion
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertTrue

class CassandraDatabaseVersionTest {

    @Test
    fun testEqualityCheck() {
        assertTrue { DatabaseVersion.fromString("3.0.0") == DatabaseVersion.fromString("3.0.0") }
    }

    @Test
    fun testListToSet() {
        val v1 = DatabaseVersion.fromString("1.2.19")
        val v2 = DatabaseVersion.fromString("1.2.19")
        val l = listOf(v1.toString(), v2.toString())
        val s = l.toSet()
        assertThat(s).hasSize(1)
    }

    @Test
    fun testMajorMinor() {
        val db1 = DatabaseVersion.fromString("3.11.14")
        assertThat(db1.releaseMajorMinor()).isEqualTo ("3.11")
        val db2 = DatabaseVersion.fromString("6.8.40")
        assertThat(db2.releaseMajorMinor()).isEqualTo ("6.8")
    }
}