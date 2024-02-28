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

package com.datastax.montecristo.sections.configuration

import com.datastax.montecristo.fileLoaders.parsers.application.JVMSettingsParser
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class JavaHeapConfigurationTest {

    private val cleanSettingsTemplate = "## Java Heap and GC Configuration\n" +
            "\n" +
            "The GC in use is : **CMS**\n" +
            "\n" +
            "**Heap size :** 8.0 GiB  \n" +
            "**New Gen :** 4.0 GiB\n" +
            "\n" +
            "GC Flags :\n" +
            "  \n" +
            "```\n" +
            "-XX:+HeapDumpOnOutOfMemoryError  \n" +
            "-XX:+UseParNewGC  \n" +
            "-Xloggc:/var/log/cassandra/gc.log\n" +
            "```\n" +
            "\n" +
            "\n"

    private val lowNewGenTemplate = "## Java Heap and GC Configuration\n" +
            "\n" +
            "The GC in use is : **CMS**\n" +
            "\n" +
            "**Heap size :** 8.0 GiB  \n" +
            "**New Gen :** 1.0 GiB\n" +
            "\n" +
            "GC Flags :\n" +
            "  \n" +
            "```\n" +
            "-XX:+HeapDumpOnOutOfMemoryError  \n" +
            "-XX:+UseParNewGC  \n" +
            "-Xloggc:/var/log/cassandra/gc.log\n" +
            "```\n" +
            "\n" +
            "\n"

    private val lowHeapTemplate = "## Java Heap and GC Configuration\n" +
            "\n" +
            "The GC in use is : **CMS**\n" +
            "\n" +
            "**Heap size :** 4.0 GiB  \n" +
            "**New Gen :** 2.0 GiB\n" +
            "\n" +
            "GC Flags :\n" +
            "  \n" +
            "```\n" +
            "-XX:+HeapDumpOnOutOfMemoryError  \n" +
            "-XX:+UseParNewGC  \n" +
            "-Xloggc:/var/log/cassandra/gc.log\n" +
            "```\n" +
            "\n" +
            "\n"

    @Test
    fun getDocumentCleanSettings() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
        "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
        "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
        "-Xloggc:/var/log/cassandra/gc.log\n" +
        "-XX:+HeapDumpOnOutOfMemoryError\n" +
        "-XX:+UseParNewGC\n" +
        "-Xms8192M\n" +
        "-Xmx8G\n" +
        "-Xmn4G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).isEqualTo(cleanSettingsTemplate)
    }

    @Test
    fun getDocumentLowNewGen() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+UseParNewGC\n" +
                "-Xms8192M\n" +
                "-Xmx8G\n" +
                "-Xmn1G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend allocating up to 50% of the total heap size to the new gen, especially in read heavy workloads. The current heap's new generation size is smaller than optimal (1.0 GiB).")
        assertThat(template).isEqualTo(lowNewGenTemplate)
    }


    @Test
    fun getDocumentLowHeap() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+UseParNewGC\n" +
                "-Xms4096M\n" +
                "-Xmx4G\n" +
                "-Xmn2G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 32L * 1024 * 1024
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings, config = config)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("Your heap size is too small (4.0 GiB). With the available 32.0 GiB of RAM, we recommend allocating between 12GiB to 16GiB of total heap size.")
        assertThat(template).isEqualTo(lowHeapTemplate)
    }

    @Test
    fun getDocumentG1Clean() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms31G\n" +
                "-Xmx31G\n" +
                "-Xmn8G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings, config = config)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentG1LowHeapInsufficientRam() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms24G\n" +
                "-Xmx24G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 32_000_000
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings, config = config)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("G1 ideally requires at least 20GiB of heap space to perform efficiently and you currently don't have enough RAM to use such heap sizes. We recommend using CMS instead which usually performs better than G1 when tuned appropriately.")
    }

    @Test
    fun getDocumentG1LowHeapLotsOfRam() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms16G\n" +
                "-Xmx16G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings, config = config)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("G1 ideally requires at least 20GiB of heap space to perform efficiently. We recommend increasing your heap size to value between 20GiB and 31GiB to maximize its performance.")
    }

    @Test
    fun getDocumentG1HighHeapLotsOfRam() {

        val cassandraCommand = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms32G\n" +
                "-Xmx32G"

        val jvmSettings = JVMSettingsParser.parse(cassandraCommand)
        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings, config = config)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("G1 heap is at or above 32GiB. Heap sizes of 32GiB and more no longer benefit from compressed ordinary object pointers (oops) which provide the largest number of addressable objects for the smallest heap size. We recommend decreasing your heap size to 31GiB to maximize its performance.")
    }

    @Test
    fun getDocumentTwoDifferentSetting() {

        val cassandraCommand1 = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms20G\n" +
                "-Xmx20G"

        val cassandraCommand2 = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms24G\n" +
                "-Xmx24G"

        val jvmSettings1 = JVMSettingsParser.parse(cassandraCommand1)
        val jvmSettings2 = JVMSettingsParser.parse(cassandraCommand2)

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node1 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings1, config = config)
        val node2 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings2, config = config)

        val nodeList: List<Node> = listOf(node1,node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend aligning the JVM settings across the nodes - there are 2 different JVM configurations.")
     }

    @Test
    fun getDocumentTwoIdenticalSetting() {

        val cassandraCommand1 = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms20G\n" +
                "-Xmx20G"

        val jvmSettings1 = JVMSettingsParser.parse(cassandraCommand1)
        val jvmSettings2 = JVMSettingsParser.parse(cassandraCommand1)

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node1 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings1, config = config)
        val node2 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings2, config = config)

        val nodeList: List<Node> = listOf(node1,node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getDocumentTwoIdenticalSettingDifferentOrder() {

        val cassandraCommand1 = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xms20G\n" +
                "-Xmx20G"

        val cassandraCommand2 = "cassandra command formatted for readability\n" +
                "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48\n" +
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java\n" +
                "-Xloggc:/var/log/cassandra/gc.log\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+G1GC\n" +
                "-Xmx20G\n" +
                "-Xms20G"

        val jvmSettings1 = JVMSettingsParser.parse(cassandraCommand1)
        val jvmSettings2 = JVMSettingsParser.parse(cassandraCommand2)

        val config = mockk<Configuration>(relaxed = true)
        every { config.memInfo.memTotal } returns 64_000_000
        val node1 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings1, config = config)
        val node2 = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", jvmSettings = jvmSettings2, config = config)

        val nodeList: List<Node> = listOf(node1,node2)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val jvm = JavaHeapConfiguration()
        val recs: MutableList<Recommendation> = mutableListOf()

        jvm.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }
}