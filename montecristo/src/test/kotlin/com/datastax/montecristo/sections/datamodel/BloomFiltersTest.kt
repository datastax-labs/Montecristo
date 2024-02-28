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

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.HumanCount
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class BloomFiltersTest {

    private val templateSingleTable = "## Bloom Filters\n" +
            "\n" +
            "Bloom filters are used by Cassandra to quickly determine if a partition key exists in a particular SSTable. The filter is kept in memory (stored off heap), and provides a probabilistic set membership test. It gives either a definitive \"does not exist\" result or \"does exist\" with a certain probability. The higher the probability of a false positive, when the filter returns true but the element is not in the set, the larger the bloom filter and the more memory it uses. \n" +
            "\n" +
            "The probability is controlled by the `bloom_filter_fp_chance` table property, which defaults to 0.10 when using LeveledCompactionStrategy and 0.01 when using SizeTieredCompactionStrategy.\n" +
            "\n" +
            "The rate of Bloom Filter false positives can be tracked via metrics and using the nodetool cfstats command. Typically we expect the rate to be close to the defined rate in the table schema.\n" +
            "\n" +
            "The amount of off-heap memory usage can be viewed per table using nodetool cfstats, or by JMX.\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Reads (Total)</div></span>|<span><div style=\"text-align:left;\">False Positive Chance</div></span>|<span><div style=\"text-align:left;\">Recent FP Rate (avg)</div></span>|<span><div style=\"text-align:left;\">Offheap Memory Used (mean)</div></span>|<span><div style=\"text-align:left;\">Disk Space (mean)</div></span>\n" +
            "---|---|---|---|---|---\n" +
            "test_table|100.0 k|10.0%|5.0%|100.0 kB|200.0 kB\n" +
            "\n"

    private val templateMultipleTable = "## Bloom Filters\n" +
            "\n" +
            "Bloom filters are used by Cassandra to quickly determine if a partition key exists in a particular SSTable. The filter is kept in memory (stored off heap), and provides a probabilistic set membership test. It gives either a definitive \"does not exist\" result or \"does exist\" with a certain probability. The higher the probability of a false positive, when the filter returns true but the element is not in the set, the larger the bloom filter and the more memory it uses. \n" +
            "\n" +
            "The probability is controlled by the `bloom_filter_fp_chance` table property, which defaults to 0.10 when using LeveledCompactionStrategy and 0.01 when using SizeTieredCompactionStrategy.\n" +
            "\n" +
            "The rate of Bloom Filter false positives can be tracked via metrics and using the nodetool cfstats command. Typically we expect the rate to be close to the defined rate in the table schema.\n" +
            "\n" +
            "The amount of off-heap memory usage can be viewed per table using nodetool cfstats, or by JMX.\n" +
            "\n" +
            "<span><div style=\"text-align:left;\">Table</div></span>|<span><div style=\"text-align:left;\">Reads (Total)</div></span>|<span><div style=\"text-align:left;\">False Positive Chance</div></span>|<span><div style=\"text-align:left;\">Recent FP Rate (avg)</div></span>|<span><div style=\"text-align:left;\">Offheap Memory Used (mean)</div></span>|<span><div style=\"text-align:left;\">Disk Space (mean)</div></span>\n" +
            "---|---|---|---|---|---\n" +
            "test_table2|100.0 k|10.0%|5.0%|100.0 kB|200.0 kB\n" +
            "test_table1|100.0 k|10.0%|5.0%|100.0 kB|200.0 kB\n" +
            "\n"

    @Test
    fun getDocumentSingleTable() {
        val tables: MutableList<Table> = mutableListOf()
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.reads } returns HumanCount(100_000)
        every { table.fpChance } returns 0.1
        every { table.readLatency.count.sum() } returns 5.0
        every { table.recentBloomFilterFalseRatio.value.isEmpty() } returns false
        every { table.recentBloomFilterFalseRatio.value.average() } returns 0.05
        every { table.bloomFilterOffHeapMemoryUsed.value.isEmpty() } returns false
        every { table.bloomFilterOffHeapMemoryUsed.value.average() } returns 100_000.0
        every { table.bloomFilterDiskSpaceUsed.value.isEmpty() } returns false
        every { table.bloomFilterDiskSpaceUsed.value.average() } returns 200_000.0
        tables.add(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val bloomFilters = BloomFilters()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = bloomFilters.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(templateSingleTable)
    }

    @Test
    fun getDocumentMultipleTables() {
        val tables: MutableList<Table> = mutableListOf()
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test_table1"
        every { table1.reads } returns HumanCount(100_000)
        every { table1.fpChance } returns 0.1
        every { table1.readLatency.count.sum() } returns 5.0
        every { table1.recentBloomFilterFalseRatio.value.isEmpty() } returns false
        every { table1.recentBloomFilterFalseRatio.value.average() } returns 0.05
        every { table1.bloomFilterOffHeapMemoryUsed.value.isEmpty() } returns false
        every { table1.bloomFilterOffHeapMemoryUsed.value.average() } returns 100_000.0
        every { table1.bloomFilterDiskSpaceUsed.value.isEmpty() } returns false
        every { table1.bloomFilterDiskSpaceUsed.value.average() } returns 200_000.0
        tables.add(table1)
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "test_table2"
        every { table2.reads } returns HumanCount(100_000)
        every { table2.fpChance } returns 0.1
        every { table2.readLatency.count.sum() } returns 25.0
        every { table2.recentBloomFilterFalseRatio.value.isEmpty() } returns false
        every { table2.recentBloomFilterFalseRatio.value.average() } returns 0.05
        every { table2.bloomFilterOffHeapMemoryUsed.value.isEmpty() } returns false
        every { table2.bloomFilterOffHeapMemoryUsed.value.average() } returns 100_000.0
        every { table2.bloomFilterDiskSpaceUsed.value.isEmpty() } returns false
        every { table2.bloomFilterDiskSpaceUsed.value.average() } returns 200_000.0
        tables.add(table2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val bloomFilters = BloomFilters()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = bloomFilters.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        // the table output is ordered based on reads latency, so the 2nd table with 25 will appear first
        assertThat(template).isEqualTo(templateMultipleTable)
    }

    @Test
    fun getDocumentSingleHighFalseFPTable() {
        val tables: MutableList<Table> = mutableListOf()
        val table = mockk<Table>(relaxed = true)
        every { table.name } returns "test_table"
        every { table.reads } returns HumanCount(100_000)
        every { table.fpChance } returns 0.1
        every { table.readLatency.count.sum() } returns 5.0
        every { table.recentBloomFilterFalseRatio.value.isEmpty() } returns false
        every { table.recentBloomFilterFalseRatio.value.average() } returns 0.20
        every { table.bloomFilterOffHeapMemoryUsed.value.isEmpty() } returns false
        every { table.bloomFilterOffHeapMemoryUsed.value.average() } returns 100_000.0
        every { table.bloomFilterDiskSpaceUsed.value.isEmpty() } returns false
        every { table.bloomFilterDiskSpaceUsed.value.average() } returns 200_000.0
        tables.add(table)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.schema.tables } returns tables

        val bloomFilters = BloomFilters()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = bloomFilters.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(template).contains("<span style=\"color:red;\">")
    }
}