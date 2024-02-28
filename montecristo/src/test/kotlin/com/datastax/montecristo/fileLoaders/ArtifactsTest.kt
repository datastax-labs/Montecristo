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

import com.datastax.montecristo.commands.DiscoveryArgs
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.nodetool.GossipInfo
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class ArtifactsTest() {

    @Test
    fun determineVersionNormal() {
        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3"

        val versionFile = listOf("ReleaseVersion: 3.0.0")
        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineVersion(false, gossipInfo, versionFile, "node1", loadErrors)
        assertThat(response).isEqualTo(DatabaseVersion.fromString("3.11.3"))
    }

    @Test
    fun determineVersionDSENormal() {
        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3.5114"
        every { gossipInfo.dseOptions } returns Utils.parseJson("{\"dse_version\":\"5.1.14\",\"workloads\":\"Cassandra\",\"workload\":\"Cassandra\",\"active\":\"true\",\"server_id\":\"00-50-56-8C-8F-55\",\"graph\":false,\"health\":1.0}")
        val versionFile = listOf("ReleaseVersion: 3.0.0.0")
        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineVersion(true, gossipInfo, versionFile, "node1", loadErrors)
        assertThat(response).isEqualTo(DatabaseVersion.fromString("5.1.14", true))
    }

    @Test
    fun determineVersionFromFile() {

        val versionFile = listOf("ReleaseVersion: 3.11.3")
        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineVersion(false, null, versionFile, "node1", loadErrors)
        assertThat(response).isEqualTo(DatabaseVersion.fromString("3.11.3"))
    }

    @Test
    fun determineDseVersionFromFile() {

        val versionFile = listOf("ReleaseVersion: 3.11.3.1234", "DSE version: 5.11.4")
        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineVersion(true, null, versionFile, "node1", loadErrors)
        assertThat(response).isEqualTo(DatabaseVersion.fromString("5.11.4", true))
    }

    @Test
    fun determineIfDSEFromGossip() {

        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3.5114"
        every { gossipInfo.dseOptions } returns Utils.parseJson("{\"dse_version\":\"5.11.4\",\"workloads\":\"Cassandra\",\"workload\":\"Cassandra\",\"active\":\"true\",\"server_id\":\"00-50-56-8C-8F-55\",\"graph\":false,\"health\":1.0}")

        val artifact = Artifacts("/")
        val response = artifact.determineIfNodeIsDSE(gossipInfo, false, emptyList())
        assertThat(response).isEqualTo(true)
    }

    @Test
    fun determineIfDSEFromYamlFile() {

        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3.5114"
        every { gossipInfo.dseOptions } returns Utils.parseJson("{}")
        val artifact = Artifacts("/")
        val response = artifact.determineIfNodeIsDSE(gossipInfo, true,emptyList())
        assertThat(response).isEqualTo(true)
    }

    @Test
    fun determineIfDSEFromVersionFileOSS() {

        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3.5114"
        every { gossipInfo.dseOptions } returns Utils.parseJson("{}")

        val artifact = Artifacts("/")
        val response = artifact.determineIfNodeIsDSE(gossipInfo, false, listOf("ReleaseVersion: 3.11.9"))
        assertThat(response).isEqualTo(false)
    }

    @Test
    fun determineIfDSEFromVersionFileDSE() {

        val gossipInfo = mockk<GossipInfo>(relaxed = true)
        every { gossipInfo.releaseVersion } returns "3.11.3.5114"
        every { gossipInfo.dseOptions } returns Utils.parseJson("{}")

        val artifact = Artifacts("/")
        val response = artifact.determineIfNodeIsDSE(gossipInfo, false, listOf("DSE version: 5.1.14", "ReleaseVersion: 3.11.3.5114"))
        assertThat(response).isEqualTo(true)
    }


    @Test
    fun determineMostCommonVersionAllTheSame() {
        val node1 = ObjectCreators.createNode("node1", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val node2 = ObjectCreators.createNode("node1", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val nodes = listOf(node1, node2)
        val errors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineMostCommonVersion(null, nodes, false, errors)
        // returns the first in the list
        assertThat(response).isEqualTo(DatabaseVersion.fromString("3.11.5"))
        assertThat(errors.size).isEqualTo(0)
    }

    @Test
    fun determineMostCommonVersionDifferences() {
        val node1 = ObjectCreators.createNode("node1", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val node2 = ObjectCreators.createNode("node2", databaseVersion = DatabaseVersion.fromString("3.11.6"))
        val node3 = ObjectCreators.createNode("node3", databaseVersion = DatabaseVersion.fromString("3.11.6"))
        val nodes = listOf(node1, node2,node3)
        val errors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineMostCommonVersion(null, nodes, false, errors)
        // returns the first in the list
        assertThat(response).isEqualTo(DatabaseVersion.fromString("3.11.6"))
        assertThat(errors.size).isEqualTo(1)
    }

    @Test
    fun determineMostCommonVersionOverride() {
        val node1 = ObjectCreators.createNode("node1", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val node2 = ObjectCreators.createNode("node2", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val node3 = ObjectCreators.createNode("node3", databaseVersion = DatabaseVersion.fromString("3.11.5"))
        val nodes = listOf(node1, node2,node3)

        val args = DiscoveryArgs()
        args.version = DatabaseVersion.fromString("3.11.6").toString()

        val errors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response = artifact.determineMostCommonVersion(args, nodes, false, errors)
        // returns the first in the list
        assertThat(response).isEqualTo(DatabaseVersion.fromString("3.11.6"))
        assertThat(errors.size).isEqualTo(1)
    }

    @Test
    fun returnOSFromOSTextFile() {
        val rawFile = "CentOS Linux release 7.4.1708 (Core) \n" +
                "NAME=\"CentOS Linux\"\n" +
                "VERSION=\"7 (Core)\"\n" +
                "ID=\"centos\"\n" +
                "ID_LIKE=\"rhel fedora\"\n" +
                "VERSION_ID=\"7\"\n" +
                "PRETTY_NAME=\"CentOS Linux 7 (Core)\"\n" +
                "ANSI_COLOR=\"0;31\"\n" +
                "CPE_NAME=\"cpe:/o:centos:centos:7\"\n" +
                "HOME_URL=\"https://www.centos.org/\"\n" +
                "BUG_REPORT_URL=\"https://bugs.centos.org/\"\n" +
                "\n" +
                "CENTOS_MANTISBT_PROJECT=\"CentOS-7\"\n" +
                "CENTOS_MANTISBT_PROJECT_VERSION=\"7\"\n" +
                "REDHAT_SUPPORT_PRODUCT=\"centos\"\n" +
                "REDHAT_SUPPORT_PRODUCT_VERSION=\"7\"\n" +
                "\n" +
                "CentOS Linux release 7.4.1708 (Core) \n" +
                "CentOS Linux release 7.4.1708 (Core) \n"
        val osInfoLines = rawFile.split("\n")
        val osInfoJsonLines = emptyList<String>()

        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response =  artifact.getOSVersion(osInfoLines , osInfoJsonLines , "test_host", loadErrors)

        // returns the first in the list
        assertThat(response).isEqualTo("CentOS Linux 7 (Core)")
        assertThat(loadErrors.size).isEqualTo(0)
    }


    @Test
    fun returnOSFromOSJsonFileNoSubOS() {
        val rawFile = "{\n" +
                "  \"os_version\" : \"3.10.0-693.11.6.el7.x86_64\"\n" +
                "}"
        val osInfoLines = emptyList<String>()
        val osInfoJsonLines = rawFile.split("\n")

        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response =  artifact.getOSVersion(osInfoLines , osInfoJsonLines , "test_host", loadErrors)

        // returns the first in the list
        assertThat(response).isEqualTo("3.10.0-693.11.6.el7.x86_64")
        assertThat(loadErrors.size).isEqualTo(0)
    }

    @Test
    fun returnOSFromOSJsonFileWithSubOS() {
        val rawFile = "{\n" +
                "\"sub_os\" : \"Ubuntu\",\n" +
                "\"os_version\" : \"16.04\"\n" +
                "}"
        val osInfoLines = emptyList<String>()
        val osInfoJsonLines = rawFile.split("\n")

        val loadErrors = mutableListOf<LoadError>()
        val artifact = Artifacts("/")
        val response =  artifact.getOSVersion(osInfoLines , osInfoJsonLines , "test_host", loadErrors)

        // returns the first in the list
        assertThat(response).isEqualTo("Ubuntu, 16.04")
        assertThat(loadErrors.size).isEqualTo(0)
    }


}