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

package com.datastax.montecristo.sections.operations


import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.metrics.ServerMetricList
import com.datastax.montecristo.model.nodetool.Info
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.schema.Keyspace
import com.datastax.montecristo.model.schema.Schema
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class TablePartitionBalanceTest {

    @Test
    fun testIsDCBalancedWhenBalancedSingleDC() {

        val cluster = mockk<Cluster>(relaxed = true)

        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 1200
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc1"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc1"
        every { node4Info.loadInBytes } returns 1100
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        every { cluster.getDCNames() } returns listOf("dc1")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2, node3, node4)
        val tablePartitionBalance = TablePartitionBalance()
        val result = tablePartitionBalance.isDataLoadBalanced(cluster)
        assertTrue(result)
    }

    @Test
    fun testIsDCBalancedWhenBalancedMultiDC() {

        val cluster = mockk<Cluster>(relaxed = true)
        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 1200
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc2"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc2"
        every { node4Info.loadInBytes } returns 1100
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        every { cluster.getDCNames() } returns listOf("dc1", "dc2")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2)
        every { cluster.getNodesFromDC("dc2") } returns listOf(node3, node4)

        val tablePartitionBalance = TablePartitionBalance()
        val result = tablePartitionBalance.isDataLoadBalanced(cluster)
        assertTrue(result)
    }

    @Test
    fun testIsDCBalancedWhenUnbalancedSingleDC() {

        val cluster = mockk<Cluster>(relaxed = true)
        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 2000
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc1"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc1"
        every { node4Info.loadInBytes } returns 1600
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        every { cluster.getDCNames() } returns listOf("dc1")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2, node3, node4)

        val tablePartitionBalance = TablePartitionBalance()
        val result = tablePartitionBalance.isDataLoadBalanced(cluster)
        assertFalse(result)
    }

    @Test
    fun testIsDCBalancedWhenUnbalancedMultiDC() {

        val cluster = mockk<Cluster>(relaxed = true)
        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 1200
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc1"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc1"
        every { node4Info.loadInBytes } returns 1600
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        every { cluster.getDCNames() } returns listOf("dc1", "dc2")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2)
        every { cluster.getNodesFromDC("dc2") } returns listOf(node3, node4)

        val tablePartitionBalance = TablePartitionBalance()
        val result = tablePartitionBalance.isDataLoadBalanced(cluster)
        assertFalse(result)
    }


    @Test
    fun testUnbalancedSingleDC() {

        // Set up the 4 nodes, 1 DC only
        val cluster = mockk<Cluster>(relaxed = true)
        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 2000
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc1"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc1"
        every { node4Info.loadInBytes } returns 1600
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        // set up the nodes / dc within the cluster
        every { cluster.getDCNames() } returns listOf("dc1")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2, node3, node4)

        val metricsServer = mockk<IMetricServer>(relaxed = true)

        // set up Table 1
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "ks1.table3"
        every { table1.getTableName() } returns "table1"
        every { table1.getKeyspace() } returns "ks1"
        every { table1.totalDiskSpace } returns 3 // this is just used for ordering, not output. The sorted by descending needs it.
        val table1PartitionCountPairList = mutableListOf(Pair("node1", 1000.0), Pair("node2", 1100.0), Pair("node3", 1200.0), Pair("node4", 1300.0))
        val table1DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 22000.0), Pair("node3", 25000.0), Pair("node4", 30000.0))
        every { metricsServer.getHistogram("ks1","table1","EstimatedPartitionCount","Value" )} returns ServerMetricList(table1PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table1","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table1DiskSpacePairList)

        // set up Table 2
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "ks1.table3"
        every { table2.getTableName() } returns "table2"
        every { table2.getKeyspace() } returns "ks1"
        every { table2.totalDiskSpace } returns 2
        val table2PartitionCountPairList = mutableListOf(Pair("node1", 500.0), Pair("node2", 1000.0), Pair("node3", 1500.0), Pair("node4", 2000.0))
        val table2DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 40000.0), Pair("node3", 60000.0), Pair("node4", 80000.0))
        every { metricsServer.getHistogram("ks1","table2","EstimatedPartitionCount","Value" )} returns ServerMetricList(table2PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table2","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table2DiskSpacePairList)

        val table3 = mockk<Table>(relaxed = true)
        every { table3.name } returns "ks1.table3"
        every { table3.getTableName() } returns "table3"
        every { table3.getKeyspace() } returns "ks1"
        every { table3.totalDiskSpace } returns 1
        val table3PartitionCountPairList = mutableListOf(Pair("node1", 1000.0), Pair("node2", 1200.0), Pair("node3", 1400.0), Pair("node4", 1600.0))
        val table3DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 25000.0), Pair("node3", 30000.0), Pair("node4", 40000.0))
        every { metricsServer.getHistogram("ks1","table3","EstimatedPartitionCount","Value" )} returns ServerMetricList(table3PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table3","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table3DiskSpacePairList)

        val tableList = listOf(table1, table2, table3)

        val fauxKeyspace = mockk<Keyspace>(relaxed = true)
        every { fauxKeyspace.name} returns "ks1"
        every { fauxKeyspace.strategy } returns "SimpleStrategy"

        val fauxSchema = mockk<Schema>(relaxed=true)
        every { fauxSchema.keyspaces } returns listOf(fauxKeyspace)
        every { fauxSchema.tables } returns tableList

        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.11")
        every { cluster.metricServer } returns metricsServer
        every { cluster.schema} returns fauxSchema

        val tablePartitionBalance = TablePartitionBalance()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val result = tablePartitionBalance.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        // table 1
        assertThat(result).contains("node1|1.0 k|21.74|20.0 kB|20.62")
        assertThat(result).contains("node2|1.1 k|23.91|22.0 kB|22.68")
        assertThat(result).contains("node3|1.2 k|26.09|25.0 kB|25.77")
        assertThat(result).contains("node4|1.3 k|28.26|30.0 kB|30.93")
        // table 2
        assertThat(result).contains("node1|500|10.00|20.0 kB|10.00")
        assertThat(result).contains("node2|1.0 k|20.00|40.0 kB|20.00")
        assertThat(result).contains("node3|1.5 k|30.00|60.0 kB|30.00")
        assertThat(result).contains("node4|2.0 k|40.00|80.0 kB|40.00")
        // table 3
        assertThat(result).contains("node1|1.0 k|19.23|20.0 kB|17.39")
        assertThat(result).contains("node2|1.2 k|23.08|25.0 kB|21.74")
        assertThat(result).contains("node3|1.4 k|26.92|30.0 kB|26.09")
        assertThat(result).contains("node4|1.6 k|30.77|40.0 kB|34.78")
    }


    @Test
    fun testUnbalancedMultiDC() {

        // Set up the 4 nodes, 1 DC only
        val cluster = mockk<Cluster>(relaxed = true)
        val node1Info = mockk<Info>(relaxed = true)
        every { node1Info.dataCenter } returns "dc1"
        every { node1Info.loadInBytes } returns 2000
        val node1 = ObjectCreators.createNode(nodeName = "node1", info = node1Info)
        val node2Info = mockk<Info>(relaxed = true)
        every { node2Info.dataCenter } returns "dc1"
        every { node2Info.loadInBytes } returns 1000
        val node2 = ObjectCreators.createNode(nodeName = "node2", info = node2Info)
        val node3Info = mockk<Info>(relaxed = true)
        every { node3Info.dataCenter } returns "dc2"
        every { node3Info.loadInBytes } returns 950
        val node3 = ObjectCreators.createNode(nodeName = "node3", info = node3Info)
        val node4Info = mockk<Info>(relaxed = true)
        every { node4Info.dataCenter } returns "dc2"
        every { node4Info.loadInBytes } returns 1600
        val node4 = ObjectCreators.createNode(nodeName = "node4", info = node4Info)

        // set up the nodes / dc within the cluster
        every { cluster.getDCNames() } returns listOf("dc1","dc2")
        every { cluster.getNodesFromDC("dc1") } returns listOf(node1, node2)
        every { cluster.getNodesFromDC("dc2") } returns listOf(node3, node4)
        val metricsServer = mockk<IMetricServer>(relaxed = true)

        // set up Table 1
        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "ks1.table3"
        every { table1.getTableName() } returns "table1"
        every { table1.getKeyspace() } returns "ks1"
        every { table1.totalDiskSpace } returns 3 // this is just used for ordering, not output. The sorted by descending needs it.
        val table1PartitionCountPairList = mutableListOf(Pair("node1", 1000.0), Pair("node2", 1100.0), Pair("node3", 1200.0), Pair("node4", 1300.0))
        val table1DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 22000.0), Pair("node3", 25000.0), Pair("node4", 30000.0))
        every { metricsServer.getHistogram("ks1","table1","EstimatedPartitionCount","Value" )} returns ServerMetricList(table1PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table1","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table1DiskSpacePairList)

        // set up Table 2
        val table2 = mockk<Table>(relaxed = true)
        every { table2.name } returns "ks1.table3"
        every { table2.getTableName() } returns "table2"
        every { table2.getKeyspace() } returns "ks1"
        every { table2.totalDiskSpace } returns 2
        val table2PartitionCountPairList = mutableListOf(Pair("node1", 500.0), Pair("node2", 1000.0), Pair("node3", 1500.0), Pair("node4", 2000.0))
        val table2DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 40000.0), Pair("node3", 60000.0), Pair("node4", 80000.0))
        every { metricsServer.getHistogram("ks1","table2","EstimatedPartitionCount","Value" )} returns ServerMetricList(table2PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table2","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table2DiskSpacePairList)

        val table3 = mockk<Table>(relaxed = true)
        every { table3.name } returns "ks1.table3"
        every { table3.getTableName() } returns "table3"
        every { table3.getKeyspace() } returns "ks1"
        every { table3.totalDiskSpace } returns 1
        val table3PartitionCountPairList = mutableListOf(Pair("node1", 1000.0), Pair("node2", 1200.0), Pair("node3", 1400.0), Pair("node4", 1600.0))
        val table3DiskSpacePairList = mutableListOf(Pair("node1", 20000.0), Pair("node2", 25000.0), Pair("node3", 30000.0), Pair("node4", 40000.0))
        every { metricsServer.getHistogram("ks1","table3","EstimatedPartitionCount","Value" )} returns ServerMetricList(table3PartitionCountPairList)
        every { metricsServer.getHistogram("ks1","table3","LiveDiskSpaceUsed","Count")} returns ServerMetricList(table3DiskSpacePairList)

        val tableList = listOf(table1, table2, table3)

        val fauxKeyspace = mockk<Keyspace>(relaxed = true)
        every { fauxKeyspace.name} returns "ks1"
        every { fauxKeyspace.strategy } returns "NetworkTopologyStrategy"
        every { fauxKeyspace.options } returns listOf(Pair("dc1",1),Pair("dc2",1))

        val fauxSchema = mockk<Schema>(relaxed=true)
        every { fauxSchema.keyspaces } returns listOf(fauxKeyspace)
        every { fauxSchema.tables } returns tableList

        every { cluster.isDse } returns false
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.11")
        every { cluster.metricServer } returns metricsServer
        every { cluster.schema} returns fauxSchema

        val tablePartitionBalance = TablePartitionBalance()
        val recs: MutableList<Recommendation> = mutableListOf()
        val searcher = mockk<Searcher>(relaxed = true)
        val result = tablePartitionBalance.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        // table 1
        assertThat(result).contains("node1|dc1|1.0 k|47.62|20.0 kB|47.62")
        assertThat(result).contains("node2|dc1|1.1 k|52.38|22.0 kB|52.38")
        assertThat(result).contains("node3|dc2|1.2 k|48.00|25.0 kB|45.45")
        assertThat(result).contains("node4|dc2|1.3 k|52.00|30.0 kB|54.55")
        // table 2
        assertThat(result).contains("node1|dc1|500|33.33|20.0 kB|33.33")
        assertThat(result).contains("node2|dc1|1.0 k|66.67|40.0 kB|66.67")
        assertThat(result).contains("node3|dc2|1.5 k|42.86|60.0 kB|42.86")
        assertThat(result).contains("node4|dc2|2.0 k|57.14|80.0 kB|57.14")
        // table 3
        assertThat(result).contains("node1|dc1|1.0 k|45.45|20.0 kB|44.44")
        assertThat(result).contains("node2|dc1|1.2 k|54.55|25.0 kB|55.56")
        assertThat(result).contains("node3|dc2|1.4 k|46.67|30.0 kB|42.86")
        assertThat(result).contains("node4|dc2|1.6 k|53.33|40.0 kB|57.14")
    }
}