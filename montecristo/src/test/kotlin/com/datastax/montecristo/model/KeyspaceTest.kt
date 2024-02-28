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

import com.datastax.montecristo.fileLoaders.parsers.schema.SchemaParser
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class KeyspaceTest {

    @Test
    fun testParseSimpleKeyspace() {
        val keyspaces = SchemaParser.getKeyspaces(listOf("CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"))
        kotlin.test.assertEquals("SimpleStrategy", keyspaces.first().strategy)
        kotlin.test.assertEquals("movielens", keyspaces.first().name)
    }

    @Test
    fun testParseNTSKeyspace() {
        val keyspaces = SchemaParser.getKeyspaces(listOf("CREATE KEYSPACE movielens WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3', 'datacenter2': '2'}  AND durable_writes = true;"))

        kotlin.test.assertEquals("movielens", keyspaces.first().name)
        kotlin.test.assertEquals("NetworkTopologyStrategy", keyspaces.first().strategy)
        kotlin.test.assertEquals(3, keyspaces.first().getDCSettings("datacenter1"))
        kotlin.test.assertEquals(2, keyspaces.first().getDCSettings("datacenter2"))
    }

    @Test
    fun isInDC() {
        val keyspaces = SchemaParser.getKeyspaces(listOf("CREATE KEYSPACE movielens WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3', 'datacenter2': '2'}  AND durable_writes = true;"))

        assertThat(keyspaces.first().isInDC("datacenter1")).isTrue()
        assertThat(keyspaces.first().isInDC("WHATEVS")).isFalse()
    }

    @Test
    fun testIsInDCList() {
        val keyspaces = SchemaParser.getKeyspaces(listOf("CREATE KEYSPACE movielens WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;"))
        val dclist1 = listOf("datacenter1", "datacenter2")
        assertThat(keyspaces.first().isInDCList(dclist1)).isTrue()

        val dclist2 = listOf("dc1", "dc2")
        assertThat(keyspaces.first().isInDCList(dclist2)).isFalse()

    }
}