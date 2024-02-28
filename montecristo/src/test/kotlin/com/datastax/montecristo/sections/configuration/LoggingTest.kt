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

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.LogFileFinder
import com.datastax.montecristo.logs.LogSettingsParser
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.model.versions.DatabaseVersion
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.testHelpers.ObjectCreators
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.File

internal class LoggingTest {

    @Test
    fun getDocumentClean() {

        // fragment of file
        val logback = "<configuration><appender name=\"DEBUGLOG\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "<file>\${cassandra.logdir}/debug.log</file>\n" +
                "</appender></configuration>"

        val logSettings = LogSettingsParser.parseLoggers (logback.split("\n"))
        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", logSettings = logSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val log = Logging()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = log.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
        assertThat(template).contains(logback)
    }

    @Test
    fun getDocumentHasStdOut() {

        // fragment of file
        val logback = "<configuration><appender name=\"STDOUT\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "<file>\${cassandra.logdir}/debug.log</file>\n" +
                "</appender></configuration>"

        val logSettings = LogSettingsParser.parseLoggers (logback.split("\n"))

        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", logSettings = logSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList

        val log = Logging()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = log.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend switching off STDOUT logging as it is unmonitored and has a performance impact. In the file above, this can be done by removing the following line: `<appender-ref ref=\"STDOUT\" />`")
        assertThat(template).contains(logback)
    }

    @Test
    fun getDocumentHasAsyncDebugIn2_2() {

        // fragment of file
        val logback = "<configuration><appender name=\"ASYNCDEBUGLOG\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "<file>\${cassandra.logdir}/debug.log</file>\n" +
                "</appender></configuration>"

        val loggingSettings = LogSettingsParser.parseLoggers(logback.split("\n"))

        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", logSettings = loggingSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("2.2.9")

        val log = Logging()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = log.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(1)
        assertThat(recs[0].priority).isEqualTo(RecommendationPriority.IMMEDIATE)
        assertThat(recs[0].longForm).isEqualTo("We recommend switching off ASYNCDEBUGLOG logging as it has a performance impact. In the file above, this can be done by removing the following line: `<appender-ref ref=\"ASYNCDEBUGLOG\" />`")
    }

    @Test
    fun getDocumentHasAsyncDebugPost2_2() {

        // fragment of file
        val logback = "    <appender name=\"ASYNCDEBUGLOG\" class=\"ch.qos.logback.core.rolling.RollingFileAppender\">\n" +
                "    <file>\${cassandra.logdir}/debug.log</file>\n" +
                "  </appender>"

        val loggingSettings = LogSettingsParser.parseLoggers(logback.split("\n"))

        val node = ObjectCreators.createNode(nodeName = "extracted/node1_artifacts_2020_10_06_1140_1601977214", logSettings = loggingSettings)
        val nodeList: List<Node> = listOf(node)
        val searcher = mockk<Searcher>(relaxed = true)
        val cluster = mockk<Cluster>(relaxed = true)
        every { cluster.nodes } returns nodeList
        every { cluster.databaseVersion } returns DatabaseVersion.fromString("3.11.9")

        val log = Logging()
        val recs: MutableList<Recommendation> = mutableListOf()

        val template = log.getDocument(cluster, searcher, recs, ExecutionProfile.default())
        assertThat(recs.size).isEqualTo(0)
    }

    @Test
    fun getLogbackConfigFindsConfig() {
        // getLogbackConfig can find the logback.xml in a mockup artefacts directory.
        val resourcesDir = File("src/test/resources/cassandraResources/3.11/artefacts")
        assertThat(LogFileFinder.getLogbackConfig(resourcesDir)).isNotEmpty
    }

    @Test
    fun logbackAppendersAreParsed() {
        // logbackAppenders are parsed out of a logback.xml.
        val testLogbackConfig = this.javaClass.getResourceAsStream("/cassandraResources/3.11/artefacts/conf/logback.xml")
                .reader()
                .readText()
                .lines()
        val logSettings = LogSettingsParser.parseLoggers(testLogbackConfig)
        assertThat(logSettings.appenders).hasSize(4)
    }
}