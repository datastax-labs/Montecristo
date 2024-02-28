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

package com.datastax.montecristo.helpers

import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.application.ConfigurationSetting
import org.apache.cassandra.exceptions.SyntaxException
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.time.LocalDateTime
import java.util.*
import kotlin.test.assertEquals

internal class UtilsTest {

    @Test
    fun parseDateAndName() {
    }

    @Test
    fun tryParseValidDate()  {
        assertThat(Utils.tryParseDate("2020-12-31T12:34:56")).isEqualTo(LocalDateTime.of(2020,12,31,12,34,56) )
    }

    @Test
    fun tryParseInvalidDate() {
        // This is what happens when we have no logs, there is no date entry, we should get back today
        // because of the millisecond difference in when .now is generated vs tested, we just check for the current date (no time)
        val returnDate = Utils.tryParseDate("")
        assertThat(returnDate.toLocalDate()).isEqualTo(LocalDateTime.now().toLocalDate())
    }

    @Test
    fun humanReadableCount() {
        assertEquals("1", Utils.humanReadableCount(1))
        assertEquals("10", Utils.humanReadableCount(10))
        assertEquals("100", Utils.humanReadableCount(100))
        assertEquals("1.0 k", Utils.humanReadableCount(1000))
        assertEquals("10.0 k", Utils.humanReadableCount(10_000))
        assertEquals("100.0 k", Utils.humanReadableCount(100_000))
        assertEquals("1.0 M", Utils.humanReadableCount(1_000_000))
        assertEquals("10.0 M", Utils.humanReadableCount(10_000_000))
        assertEquals("100.0 M", Utils.humanReadableCount(100_000_000))
        assertEquals("1.0 B", Utils.humanReadableCount(1_000_000_000))
        assertEquals("10.0 B", Utils.humanReadableCount(10_000_000_000))
        assertEquals("100.0 B", Utils.humanReadableCount(100_000_000_000))
        assertEquals("1.0 T", Utils.humanReadableCount(1_000_000_000_000))
        assertEquals("10.0 T", Utils.humanReadableCount(10_000_000_000_000))
        // add a few in to check rounding
        assertEquals("1.4 k", Utils.humanReadableCount(1400))
        assertEquals("1.4 k", Utils.humanReadableCount(1449))
        assertEquals("1.5 k", Utils.humanReadableCount(1450))
        assertEquals("1.5 k", Utils.humanReadableCount(1499))
    }

    @Test
    fun parseJson() {
        val jsonNode = Utils.parseJson("{ \"testString\" : \"test_value\"}")
        assertThat(jsonNode.get("testString").textValue()).isEqualTo("test_value")
    }

    @Test
    fun round() {
        // this routine is sligthly misleading in name - it is rounding at 2 decimal places
        assertEquals(10.00, Utils.round(10.0))
        assertEquals(3.14, Utils.round(3.1415))
        assertEquals(1.5, Utils.round(1.5))


        assertEquals(1.55, Utils.round(1.551))
        assertEquals(1.56, Utils.round(1.559))

        assertEquals(2.00, Utils.round(1.9999))
        // check odd / even bias
        assertEquals(2.50, Utils.round(2.505))
        assertEquals(3.50, Utils.round(3.505))

    }

    @Test
    fun displayInconsistentConfig() {
        val configMap = mutableMapOf<String,ConfigValue>()
        configMap.put("node1",ConfigValue(true, "","some_value"))
        configMap.put("node2",ConfigValue(true, "","another_value"))
        assertThat( Utils.displayInconsistentConfig(configMap.entries)).isEqualTo("some_value : 1 node\nanother_value : 1 node\n  \nnode1 = some_value\nnode2 = another_value\n")
    }

    @Test
    fun formatCassandraYamlSettingNotConsistent() {
        var values: Map<String, ConfigValue> = mapOf(Pair("node1", ConfigValue(true, "","value_1")), Pair("node2",ConfigValue(true, "","value_2")))
        val cs = ConfigurationSetting("test", values)
        assertThat(Utils.formatCassandraYamlSetting(cs)).isEqualTo("`test` setting is inconsistent across the cluster: \n\n```\nvalue_1 : 1 node\nvalue_2 : 1 node\n  \nnode1 = value_1\nnode2 = value_2\n```\n\n")
    }

    @Test
    fun formatCassandraYamlSettingConsistent() {
        var values: Map<String, ConfigValue> = mapOf(Pair("node1",ConfigValue(true, "","same_value")), Pair("node2",ConfigValue(true, "","same_value")))
        val cs = ConfigurationSetting("test", values)
        assertThat(Utils.formatCassandraYamlSetting(cs)).isEqualTo("`test` is configured as follows: \n\n```\ntest: same_value \n```\n\n")

    }

    @Test
    fun parseGoodCQL() {
        var result = Utils.parseCQL("CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;")
        // we can check much other than it did not error and it did not return null
        assertThat(result).isNotNull
    }

    @Test(expected = SyntaxException::class)
    fun parseBadCQL() {
        var result = Utils.parseCQL("Lorem ipsum dolor sit amet, consectetur adipiscing elit")
    }

    @Test
    fun stripCustomTypesFromCQL() {

        var result = Utils.stripCustomJavaTypes ("CREATE TABLE crm.\"GeoServerFeature_p\" (\n" +
                "    community_id int,\n" +
                "    member_id bigint,\n" +
                "    geo_server_line 'org.apache.cassandra.db.marshal.LineStringType',\n" +
                "    geo_server_point 'org.apache.cassandra.db.marshal.PointType',\n" +
                "    PRIMARY KEY (community_id)\n" +
                ") WITH CLUSTERING ORDER BY (member_id ASC)\n" +
                "    AND bloom_filter_fp_chance = 0.01\n" +
                "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                "    AND comment = ''\n" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                "    AND crc_check_chance = 1.0\n" +
                "    AND default_time_to_live = 0\n" +
                "    AND gc_grace_seconds = 864000\n" +
                "    AND max_index_interval = 2048\n" +
                "    AND memtable_flush_period_in_ms = 0\n" +
                "    AND min_index_interval = 128\n" +
                "    AND nodesync = {'enabled': 'true'}\n" +
                "    AND speculative_retry = '99PERCENTILE';\n")
        assertThat(result).isEqualTo("CREATE TABLE crm.\"GeoServerFeature_p\" (\n" +
                "    community_id int,\n" +
                "    member_id bigint,\n" +
                "    geo_server_line blob,\n" + // these two lines are the only
                "    geo_server_point blob,\n" + // part of the string that should change.
                "    PRIMARY KEY (community_id)\n" +
                ") WITH CLUSTERING ORDER BY (member_id ASC)\n" +
                "    AND bloom_filter_fp_chance = 0.01\n" +
                "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                "    AND comment = ''\n" +
                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                "    AND crc_check_chance = 1.0\n" +
                "    AND default_time_to_live = 0\n" +
                "    AND gc_grace_seconds = 864000\n" +
                "    AND max_index_interval = 2048\n" +
                "    AND memtable_flush_period_in_ms = 0\n" +
                "    AND min_index_interval = 128\n" +
                "    AND nodesync = {'enabled': 'true'}\n" +
                "    AND speculative_retry = '99PERCENTILE';\n")
    }

    @Test
    fun parseUDTWithCustomType() {
        val result = Utils.parseCQL("CREATE TYPE test.my_custom_udt (\n" +
                "    policy text,\n" +
                "    pattern 'org.apache.cassandra.db.marshal.PointType',\n" +
                "    scope text\n" +
                ");")
        // if this parses without error, it worked
        assertThat(result).isNotNull
    }

}