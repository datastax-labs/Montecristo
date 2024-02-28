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

package com.datastax.montecristo.metrics

import org.junit.Assert.assertEquals
import org.junit.Test

class ParserTest {
    var parser = MetricParser()


    @Test
    fun testBasicMetricTypes() {
        var tests = mapOf(
                "org.apache.cassandra.internal"
                        to "org.apache.cassandra.internal{type=MemtableReclaimMemory}[]CoreThreads: 1",
                "org.apache.cassandra.metrics"
                        to "org.apache.cassandra.metrics{type=Table, keyspace=system_schema, scope=indexes, name=LiveScannedHistogram}[]95thPercentile: 0.0",
                "org.apache.cassandra.db"
                        to "org.apache.cassandra.db{type=ColumnFamilies, keyspace=system_schema, columnfamily=types}[]ColumnFamilyName: types")

        for((expected, s) in tests) {
            var parsed = parser.getParsedExpression(s)
            assertEquals(expected, parsed.metric)
        }
    }


    @Test
    fun testParseTableHistogram() {
        var metric = "org.apache.cassandra.metrics{type=Table, keyspace=system_schema, scope=indexes, name=LiveScannedHistogram}[]95thPercentile: 0.0"

        val histo = parser.parseMetric(metric, "node1")
        histo.getPrepared()
    }

    // added because the parser broke on this one with Tenable
    @Test
    fun testCompationParametersJson() {
        var test = "org.apache.cassandra.db{type=Tables, keyspace=system, table=size_estimates}[]CompactionParametersJson: {\"min_threshold\":\"4\",\"max_threshold\":\"32\",\"class\":\"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy\"}"
        val parsed = parser.getParsedExpression(test)
        assertEquals("CompactionParametersJson", parsed.key)
    }

    @Test
    fun testMaxPartition() {
        var test = "org.apache.cassandra.metrics{type=Table, keyspace=us_2a_scan, scope=attachment_by_plugin_host, name=MaxPartitionSize}[]Value: 962624926"
        val parsed = parser.getParsedExpression(test)
        assertEquals("MaxPartitionSize", parsed.properties.get("name"))
    }
}