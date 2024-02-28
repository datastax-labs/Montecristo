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

package com.datastax.montecristo.sections.astra

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.metrics.ServerMetricList
import com.datastax.montecristo.model.nodetool.Info
import com.datastax.montecristo.model.schema.Keyspace
import com.datastax.montecristo.model.schema.Schema
import com.datastax.montecristo.model.schema.Table
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class OperationsTest {

    private val schema = mockk<Schema>(relaxed = true)
    private val logSearch = mockk<Searcher>(relaxed = true)
    private val blockedTasks = mockk<BlockedTasks>(relaxed = true)
    private val metricServer = mockk<IMetricServer>(relaxed = true)

    @Test
    fun readsSimpleScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)
        assertThat(results.get("test.foo")?.one).isEqualTo(3000000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1500000)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1500000)
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readSimpleScenario2Hours() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 7200)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)
        assertThat(results.get("test.foo")?.one).isEqualTo(1500000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(750000)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(750000)
        assertThat(results.get("test.foo")?.all).isEqualTo(500000)
    }

    @Test
    fun readLocalRepairScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.1"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // CL results
        // ONE - 2.5m reads, would be 2.5m x 1 = 2.5m + 10% (250k) with 2 additional reads, adding 500k more reads to get 3m
        // LOCAL_Q - 1,428,571 reads * 2 = 2857142 + 10% (142,857) with 1 additional read = 3m (rounding is going to make this not quite right)
        // Q = same as local Q on this test
        // ALL - RR has no impact, 1m reads.
        assertThat(results.get("test.foo")?.one).isEqualTo(2500000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1428571)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1428571)
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readGlobalReadRepairNoImpactScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.1"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 1 DC - global RR has no impact
        assertThat(results.get("test.foo")?.one).isEqualTo(3000000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1500000)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1500000)
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readSimple2DCScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 2 DC
        assertThat(results.get("test.foo")?.one).isEqualTo(6000000)
        // local quorum means each DC is considered separately, so the 6m reads is considered 2 separate DCs with 3m reads each, at quorum is 1.5m each so 3m total
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(3000000)
        // quorum is 4 reads per operation, so 1.5m operations is 6m reads
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1500000)
        // 1m reads, 6 replicas read per read
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readTwoDCWithKSReplicatedIn1OnlyScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 2 DC
        assertThat(results.get("test.foo")?.one).isEqualTo(3000000)
        // local quorum means each DC is considered separately, so the 3m in 1 DC, at quorum is 1.5m each reads
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1500000)
        // quorum is the same as local quorum in this scenario
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1500000)
        // 1m reads, 3 replicas read per read
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readSimple3DCScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))
        readData.add(Pair("node7", 1000000.0))
        readData.add(Pair("node8", 1000000.0))
        readData.add(Pair("node9", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)
        val info3 = Info("dc3", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)
        val node7 = ObjectCreators.createNode("node7", info = info3)
        val node8 = ObjectCreators.createNode("node8", info = info3)
        val node9 = ObjectCreators.createNode("node9", info = info3)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6,node7,node8,node9)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3), Pair("dc3",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 3 DC
        assertThat(results.get("test.foo")?.one).isEqualTo(9000000)
        // local quorum means each DC is considered separately, so the 9m reads is considered 3 separate DCs with 3m reads each, at quorum is 1.5m each so 4.5m total
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(4500000)
        // quorum is 5 reads per operation, so 1.5m operations is 6m reads
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1800000)
        // 1m reads, 9 replicas read per read
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readUnbalanced3DCScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)
        val info3 = Info("dc3", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info3)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",2), Pair("dc3",1)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 3 DC - but unbalanced as 3 nodes, 2 nodes, 1 node
        assertThat(results.get("test.foo")?.one).isEqualTo(6000000)
        // local quorum means each DC is considered separately, so the 6m reads is considered 3 separate DCs with 1.5m , 1m and 1m respectively - so 3.5m total
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(3500000)
        // quorum is 4 reads per operation, so 1.5m operations is 6m reads
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1500000)
        // 1m reads, 6 replicas read per read
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readGlobalReadRepairSimpleScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.1"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 2 DC - this is now getting complex!
        // ONE - 4m reads, would be 4m x 1 = 4m + 10% (400k) with 5 additional reads, adding 2m more reads to get 6m
        // LOCAL_Q - 2.5m reads reads * 2 = 5m + 10% (250k) with 4 additional read = 1m
        // Q = 1428571 reads * 4 = 5,714,284, 10% (142,857) with 2 additional reads = 285714, gives 6m reads (rounding means there are decimals not expressed here)
        // ALL - RR has no impact, 1m reads.
        assertThat(results.get("test.foo")?.one).isEqualTo(4000000)
        // local quorum means each DC is considered separately, so the 6m reads is considered 2 separate DCs with 3m reads each, at quorum is 1.5m each so 3m total
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(2500000)
        // quorum is 4 reads per operation, so 1.5m operations is 6m reads
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1428571)
        // 1m reads, 6 replicas read per read, RR has no impact
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readBothReadRepairScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.1"
        every { table1.dcLocalReadRepair } returns "0.1"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // 2 DC - this is now getting complex!
        // ONE - 3,571,429 reads, would be this x 1 = 3571429 + 10% (357142.9k) with 5 additional reads and 9% (321428.6) with 2 additional reads = 6m
        // LOCAL_Q - 2409639 reads * 2 = 4819278 + 10% (240963.9) with 4 additional read = 963855.6 and 9% with 1 additional read (216867) = 6m
        // Q = 1428571 reads * 4 = 5,5,714,284, 10% (142,857.1) with 2 additional reads = 285714.2, local RR is ignored because its quorum on multi dc, total = 6m reads
        // ALL - RR has no impact, 1m reads.
        assertThat(results.get("test.foo")?.one).isEqualTo(3571429)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(2409639)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1428571)
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun readSingleNodeDCRRShouldHaveNoImpactScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.readRepair } returns "0.1"
        every { table1.dcLocalReadRepair } returns "0.1"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)

        val nodelist = listOf(node1)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 1)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)
        assertThat(results.get("test.foo")?.one).isEqualTo(1000000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1000000)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1000000)
        assertThat(results.get("test.foo")?.all).isEqualTo(1000000)
    }

    @Test
    fun writeSimple2DCScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.writeLatency.count } returns ServerMetricList(readData)
        every { table1.liveDiskSpaceUsed.count.sum() } returns 6000000.0
        every { table1.compressionRatio.value.average()} returns 0.5
        every { table1.compressionRatio.value.getData() } returns mapOf(Pair("node1",0.5), Pair("node2",-1.0))
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)


        val operations = Operations()
        val results = operations.calculateTableWriteOperations(tableList, keyspaceList, cluster)

        // total RF is 6, so 6m writes / 6 = 1m write operations
        assertThat(results.get("test.foo")?.writes).isEqualTo(1000000)
        // 6 MB total data, compression raito 0.5, so 12 MB total
        assertThat(results.get("test.foo")?.totalSpaceUsedUncompressed).isEqualTo(12000000.0)
        // 6 MB, 2 DC, rf 3 in each, 1 MB actual data, compression ratio of 0.5 so 2 MB = 1 replica
        assertThat(results.get("test.foo")?.nonRFSpaceUsedCompressed).isEqualTo(1000000.0)
        assertThat(results.get("test.foo")?.nonRFSpaceUsedUncompressed).isEqualTo(2000000.0)
    }

    @Test
    fun write2DCReplicatedIn1OnlyScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.writeLatency.count } returns ServerMetricList(readData)
        every { table1.liveDiskSpaceUsed.count.sum() } returns 6000000.0
        every { table1.compressionRatio.value.getData() } returns mapOf(Pair("test",0.5))
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 3600)
        val info2 = Info("dc2", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableWriteOperations(tableList, keyspaceList, cluster)

        // total RF is 3, so 3m writes / 3 = 1m write operations
        assertThat(results.get("test.foo")?.writes).isEqualTo(1000000)
        // 6MB, 1 DC, 0.5 compression ratio, 12 MB total data
        assertThat(results.get("test.foo")?.totalSpaceUsedUncompressed).isEqualTo(12000000.0)
        // 6 MB, 1 DC, rf 3, 2 MB actual data, 0.5 compression, 4 MB
        assertThat(results.get("test.foo")?.nonRFSpaceUsedCompressed).isEqualTo(2000000.0)
        assertThat(results.get("test.foo")?.nonRFSpaceUsedUncompressed).isEqualTo(4000000.0)
    }

    @Test
    fun writeSimple2DCScenario2Hours() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))
        readData.add(Pair("node4", 1000000.0))
        readData.add(Pair("node5", 1000000.0))
        readData.add(Pair("node6", 1000000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.writeLatency.count } returns ServerMetricList(readData)
        every { table1.liveDiskSpaceUsed.count.sum() } returns 6000000.0
        every { table1.compressionRatio.value.getData() } returns mapOf(Pair("test",0.5))
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info1 = Info("dc1", "", 0, 7200)
        val info2 = Info("dc2", "", 0, 7200)

        val node1 = ObjectCreators.createNode("node1", info = info1)
        val node2 = ObjectCreators.createNode("node2", info = info1)
        val node3 = ObjectCreators.createNode("node3", info = info1)
        val node4 = ObjectCreators.createNode("node4", info = info2)
        val node5 = ObjectCreators.createNode("node5", info = info2)
        val node6 = ObjectCreators.createNode("node6", info = info2)

        val nodelist = listOf(node1, node2, node3,node4,node5,node6)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3), Pair("dc2",3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableWriteOperations(tableList, keyspaceList, cluster)

        // total RF is 3, so 3m writes / 3 = 1m write operations in 2 hours, 500k in 1 hour.
        assertThat(results.get("test.foo")?.writes).isEqualTo(500000)
        // 6 mb, 0.5 compression, 12mb total across nodes
        assertThat(results.get("test.foo")?.totalSpaceUsedUncompressed).isEqualTo(12000000.0)
        // 6 MB, 2 DC, rf 3 in each, 1 MB actual data, 0.5 compression, 2mb total
        assertThat(results.get("test.foo")?.nonRFSpaceUsedCompressed).isEqualTo(1000000.0)
        assertThat(results.get("test.foo")?.nonRFSpaceUsedUncompressed).isEqualTo(2000000.0)

    }

    @Test
    fun readsPaxosSimpleScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val paxosData = mutableListOf<Pair<String, Double>>()
        paxosData.add(Pair("node1", 50000.0))
        paxosData.add(Pair("node2", 50000.0))
        paxosData.add(Pair("node3", 50000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.casPrepareLatency.count } returns ServerMetricList(paxosData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.0"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)
        // 3m reads, but each node had 50k of paxos, meaning 100k reads per node should be ignored, CL-1 thus is 2.7m
        assertThat(results.get("test.foo")?.one).isEqualTo(2700000)
        // If I perform 1.35m reads at LQ - of which 50k of them per node are CAS - that's not saying 50k are cas, because its 3 nodes at lq, that means 75k are cas.
        // 1.275m are LQ and generate 2 reads = 2.55m
        // 75k of them are LQ / CAS and generate 6 reads. = 450k - would of generated 150k cas metrics (50k per node)
        // total 3m reads.
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1350000)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1350000)
        // 1m reads at all, deduct 1k per node because of paxos extra reads, 900k result.
        assertThat(results.get("test.foo")?.all).isEqualTo(900000)
    }

    @Test
    fun readsPaxosWithLocalReadRepairScenario() {

        val readData = mutableListOf<Pair<String, Double>>()
        readData.add(Pair("node1", 1000000.0))
        readData.add(Pair("node2", 1000000.0))
        readData.add(Pair("node3", 1000000.0))

        val paxosData = mutableListOf<Pair<String, Double>>()
        paxosData.add(Pair("node1", 50000.0))
        paxosData.add(Pair("node2", 50000.0))
        paxosData.add(Pair("node3", 50000.0))

        val table1 = mockk<Table>(relaxed = true)
        every { table1.name } returns "test.foo"
        every { table1.readLatency.count } returns ServerMetricList(readData)
        every { table1.casPrepareLatency.count } returns ServerMetricList(paxosData)
        every { table1.readRepair } returns "0.0"
        every { table1.dcLocalReadRepair } returns "0.1"
        every { table1.getKeyspace() } returns "test"
        val tableList = listOf(table1)

        val info = Info("dc1", "", 0, 3600)

        val node1 = ObjectCreators.createNode("node1", info = info)
        val node2 = ObjectCreators.createNode("node2", info = info)
        val node3 = ObjectCreators.createNode("node3", info = info)
        val nodelist = listOf(node1, node2, node3)

        val cluster = Cluster(nodelist, false, false, DatabaseVersion.latest311(), schema, blockedTasks, metricServer, mutableListOf<LoadError>())

        val keyspace1 = Keyspace("test", listOf(Pair("dc1", 3)), "NetworkTopologyStrategy")
        val keyspaceList = listOf(keyspace1)

        val operations = Operations()
        val results = operations.calculateTableReadOperations(tableList, keyspaceList, cluster)

        // This is another complicated set of results.

        // CL results
        // ONE - For CL 1, the paxos is easier, of the 1m reads per node, 100k of them were generated extra due to the 50k paxos, so its really only 900k reads per node. 2.7m total
        //     - If I perform 2.25m reads at CL=1, that would be 2.25m x 1 = 2.25m + 10% (225k) with 2 additional reads, adding 450k more reads = 2.7m

        // LOCAL_Q - If I perform 1,285,714 reads at LQ - of which 75k are CAS that gives the following:
        // LQ Reads = 1,210,714 which generate 2 reads each         = 2,421,428
        // LQ RR Reads = 10% of the LQ Reads generate 1 extra read  = 121071.4
        // CAS - 75k Cas generated 6 reads                          = 450k
        // CAS / RR - 10% of the CAS would of triggered 1 extra read= 7.5k
        // total 3m reads.

        // Q = same as local Q on this test
        // ALL - RR has no impact, cas generated 100k extra reads, so take them off

        assertThat(results.get("test.foo")?.one).isEqualTo(2250000)
        assertThat(results.get("test.foo")?.localQuorum).isEqualTo(1285714)
        assertThat(results.get("test.foo")?.quorum).isEqualTo(1285714)
        assertThat(results.get("test.foo")?.all).isEqualTo(900000)
    }
}
