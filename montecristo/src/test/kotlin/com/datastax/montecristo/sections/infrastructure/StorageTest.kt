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

package com.datastax.montecristo.sections.infrastructure

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.storage.DfSize
import com.datastax.montecristo.model.storage.DiskDevice
import com.datastax.montecristo.model.storage.LsBlk
import com.datastax.montecristo.sections.structure.*
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class StorageTest {

    @Test
    fun getDocumentSettingsConsistent() {

        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "[kyber]"
        val dfSize = mockk<DfSize>(relaxed=true)
        every { dfSize.usedSpacePercentage("dm-0") } returns 49
        val storageDevice = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice.dfSize } returns dfSize

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|dm-0|16|[kyber]|xfs|false|51.0")
        assertThat(template).contains("node2|dm-0|16|[kyber]|xfs|false|51.0")
    }

    @Test
    fun getDocumentSettingsInconsistent() {
        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "ext4"
        val diskDevice1 = mockk<DiskDevice> (relaxed = true)
        every { diskDevice1.readAhead } returns 128
        every { diskDevice1.scheduler } returns "[none] kyber"
        val diskDevice2 = mockk<DiskDevice> (relaxed = true)
        every { diskDevice2.readAhead } returns 4096
        every { diskDevice2.scheduler } returns "[kyber]"
        val dfSize = mockk<DfSize>(relaxed=true)
        every { dfSize.usedSpacePercentage("dm-0") } returns 60
        val storageDevice1 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice1.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice1.diskDevices["dm-0"] } returns diskDevice1
        every { storageDevice1.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice1.dfSize } returns dfSize
        val storageDevice2 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice2.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice2.diskDevices["dm-0"] } returns diskDevice2
        every { storageDevice2.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice2.dfSize } returns dfSize

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice2)

        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(4)
        assertThat(recs[0].longForm).isEqualTo("We recommend lowering the read ahead for the data devices to an RA value of 32, which will result in a read ahead of 16 kb.")
        assertThat(recs[1].longForm).isEqualTo("We recommend using the XFS format for the data devices.")
        assertThat(recs[2].longForm).isEqualTo("We recommend using the kyber IO scheduler for the devices holding data.")
        assertThat(recs[3].longForm).isEqualTo("We recommend having at least 50% of the disk space available on the data volume to ensure that compaction has sufficient space to operate. 2 data volumes report less than 50% space free. We recommend further evaluation against the number of tables and their compaction strategies to ensure availability is maintained and capacity planning happens in advance.")
        assertThat(template).contains("node1|dm-0|128|[none] kyber|ext4|false|40.0")
        assertThat(template).contains("node2|dm-0|4096|[kyber]|ext4|false|40.0")
    }

    @Test
    fun getDocumentMultiDataDirectories() {
        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "[kyber]"
        val dfSize = mockk<DfSize>(relaxed=true)
        every { dfSize.usedSpacePercentage("dm-0") } returns 40
        every { dfSize.usedSpacePercentage("dm-1") } returns 40
        val storageDevice1 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice1.storageLocations.dataLocation() } returns "dm-0,dm-1"
        every { storageDevice1.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice1.diskDevices["dm-1"] } returns diskDevice
        every { storageDevice1.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice1.lsBlk.get("dm-1") } returns lsblk
        every { storageDevice1.dfSize } returns dfSize

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice1)

        val nodeList: List<Node> = listOf(node1)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|dm-0|16|[kyber]|xfs|false|60.0")
        assertThat(template).contains("node1|dm-1|16|[kyber]|xfs|false|60.0")
    }

    @Test
    fun getDocumentWrongSchedulerUsedKyberAvailable() {

        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "node [deadline] kyber bfq"
        val dfSize = mockk<DfSize>(relaxed=true)
        every { dfSize.usedSpacePercentage("dm-0") } returns 49
        val storageDevice = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice.dfSize } returns dfSize

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend using the kyber IO scheduler for the devices holding data.")
        assertThat(template).contains("node1|dm-0|16|node [deadline] kyber bfq|xfs|false|51.0")
        assertThat(template).contains("node2|dm-0|16|node [deadline] kyber bfq|xfs|false|51.0")
    }

    @Test
    fun getDocumentWrongSchedulerUsedKyberNotAvailable() {

        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "node [deadline] bfq"
        val dfSize = mockk<DfSize>(relaxed=true)
        every { dfSize.usedSpacePercentage("dm-0") } returns 49
        val storageDevice = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice.dfSize } returns dfSize

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice)
        val nodeList: List<Node> = listOf(node1, node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains("node1|dm-0|16|node [deadline] bfq|xfs|false|51.0")
        assertThat(template).contains("node2|dm-0|16|node [deadline] bfq|xfs|false|51.0")
    }

    @Test
    fun getDocumentInsufficientSpaceOnlyLevel50() {
        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "[kyber]"

        val dfSize1 = mockk<DfSize>(relaxed=true)
        every { dfSize1.usedSpacePercentage("dm-0") } returns 57
        val storageDevice1 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice1.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice1.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice1.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice1.dfSize } returns dfSize1
        val dfSize2 = mockk<DfSize>(relaxed=true)
        every { dfSize2.usedSpacePercentage("dm-0") } returns 56
        val storageDevice2 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice2.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice2.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice2.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice2.dfSize } returns dfSize2
        val dfSize3 = mockk<DfSize>(relaxed=true)
        every { dfSize3.usedSpacePercentage("dm-0") } returns 55
        val storageDevice3 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice3.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice3.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice3.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice3.dfSize } returns dfSize3

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice2)
        val node3 = ObjectCreators.createNode(nodeName = "node3", storage = storageDevice3)

        val nodeList: List<Node> = listOf(node1, node2, node3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend having at least 50% of the disk space available on the data volume to ensure that compaction has sufficient space to operate. 3 data volumes report less than 50% space free. We recommend further evaluation against the number of tables and their compaction strategies to ensure availability is maintained and capacity planning happens in advance.")
        assertThat(template).contains("node1|dm-0|16|[kyber]|xfs|false|43.0")
        assertThat(template).contains("node2|dm-0|16|[kyber]|xfs|false|44.0")
        assertThat(template).contains("node3|dm-0|16|[kyber]|xfs|false|45.0")
    }

    @Test
    fun getDocumentInsufficientSpaceOnlyLevel60() {
        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "[kyber]"

        val dfSize1 = mockk<DfSize>(relaxed=true)
        every { dfSize1.usedSpacePercentage("dm-0") } returns 67
        val storageDevice1 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice1.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice1.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice1.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice1.dfSize } returns dfSize1
        val dfSize2 = mockk<DfSize>(relaxed=true)
        every { dfSize2.usedSpacePercentage("dm-0") } returns 66
        val storageDevice2 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice2.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice2.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice2.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice2.dfSize } returns dfSize2
        val dfSize3 = mockk<DfSize>(relaxed=true)
        every { dfSize3.usedSpacePercentage("dm-0") } returns 65
        val storageDevice3 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice3.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice3.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice3.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice3.dfSize } returns dfSize3

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice2)
        val node3 = ObjectCreators.createNode(nodeName = "node3", storage = storageDevice3)

        val nodeList: List<Node> = listOf(node1, node2, node3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend having at least 50% of the disk space available on the data volume to ensure that compaction has sufficient space to operate. Operating 30%-40% available space risks compaction failing and nodes becoming unstable. 3 data volumes report less than 40% space free. We recommend reviewing the nodes and either provision additional disk space or free up space.")
        assertThat(template).contains("node1|dm-0|16|[kyber]|xfs|false|33.0")
        assertThat(template).contains("node2|dm-0|16|[kyber]|xfs|false|34.0")
        assertThat(template).contains("node3|dm-0|16|[kyber]|xfs|false|35.0")
    }

    @Test
    fun getDocumentInsufficientSpaceAllLevels() {
        val lsblk = mockk<LsBlk>(relaxed = true)
        every { lsblk.isRotational } returns false
        every { lsblk.fsType } returns "xfs"
        val diskDevice = mockk<DiskDevice> (relaxed = true)
        every { diskDevice.readAhead } returns 16
        every { diskDevice.scheduler } returns "[kyber]"

        val dfSize1 = mockk<DfSize>(relaxed=true)
        every { dfSize1.usedSpacePercentage("dm-0") } returns 75
        val storageDevice1 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice1.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice1.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice1.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice1.dfSize } returns dfSize1
        val dfSize2 = mockk<DfSize>(relaxed=true)
        every { dfSize2.usedSpacePercentage("dm-0") } returns 65
        val storageDevice2 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice2.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice2.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice2.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice2.dfSize } returns dfSize2
        val dfSize3 = mockk<DfSize>(relaxed=true)
        every { dfSize3.usedSpacePercentage("dm-0") } returns 55
        val storageDevice3 = mockk<com.datastax.montecristo.model.storage.Storage>(relaxed=true)
        every { storageDevice3.storageLocations.dataLocation() } returns "dm-0"
        every { storageDevice3.diskDevices["dm-0"] } returns diskDevice
        every { storageDevice3.lsBlk.get("dm-0") } returns lsblk
        every { storageDevice3.dfSize } returns dfSize3

        val node1 = ObjectCreators.createNode(nodeName = "node1", storage = storageDevice1)
        val node2 = ObjectCreators.createNode(nodeName = "node2", storage = storageDevice2)
        val node3 = ObjectCreators.createNode(nodeName = "node3", storage = storageDevice3)

        val nodeList: List<Node> = listOf(node1, node2, node3)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val storage = Storage()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = storage.getDocument(cluster, searcher, recs,ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].longForm).isEqualTo("We recommend having at least 50% of the disk space available on the data volume to ensure that compaction has sufficient space to operate. Operating at less than 30% available space is likely to result in compaction failing and nodes becoming unstable. 1 data volume reports less than 30% space free. 1 data volume reports less than 40% space free. 1 data volume reports less than 50% space free. We recommend reviewing the nodes and either provision additional disk space or free up space.")
        assertThat(template).contains("node1|dm-0|16|[kyber]|xfs|false|25.0")
        assertThat(template).contains("node2|dm-0|16|[kyber]|xfs|false|35.0")
        assertThat(template).contains("node3|dm-0|16|[kyber]|xfs|false|45.0")
    }
}