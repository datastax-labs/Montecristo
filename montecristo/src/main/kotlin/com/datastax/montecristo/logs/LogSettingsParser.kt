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

package com.datastax.montecristo.logs

import com.datastax.montecristo.model.logs.LogLevel
import com.datastax.montecristo.model.logs.LogSettings
import com.datastax.montecristo.model.logs.LogbackAppender
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

fun NodeList.toList(): List<Node> = IntRange(0, this.length).mapNotNull{ i->this.item(i) }
fun NodeList.getChildrenByTagName(tagName: String): List<Node> = this.toList().filter { i-> i.nodeName == tagName }

class LogSettingsParser {
    companion object {
        fun parseLoggers(logbackSettings: List<String>): LogSettings {
            if (logbackSettings.isEmpty()) {
                val logbackAppender = LogbackAppender("SYSTEMLOG", "ch.qos.logback.core.rolling.RollingFileAppender", listOf("\${cassandra.logdir}/system.log"),
                    LogLevel.INFO, "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
                return LogSettings(listOf(logbackAppender), emptyList())
            }
            val logbackSettingsXml: Document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(InputSource(logbackSettings.joinToString("\n").reader()))

            val xpFactory = XPathFactory.newInstance()
            val xPath = xpFactory.newXPath()

            val potentialRootLevelNode = xPath.evaluate("/configuration/root", logbackSettingsXml, XPathConstants.NODE)
            val rootLoggerLevel = if (potentialRootLevelNode != null) {
                val rootLevel = (potentialRootLevelNode as Node).attributes.getNamedItem("level").textContent ?: "INFO"
                // check if this value can be converted to the enum - if not, we will assume INFO, otherwise we will cause a fatal exception later
                if (enumValues<LogLevel>().any { it.name == rootLevel }) {
                    LogLevel.valueOf(rootLevel)
                } else {
                    LogLevel.valueOf("INFO")
                }
            } else {
                LogLevel.valueOf("INFO")
            }

            val appenderNodes : NodeList = xPath.evaluate("/configuration/appender", logbackSettingsXml, XPathConstants.NODESET ) as NodeList
            val logbackAppenders: List<LogbackAppender> = appenderNodes.toList()
                .map { // For each appender, build a LogbackAppender.
                    LogbackAppender(
                        name = it.attributes.getNamedItem("name")?.textContent ?: "",
                        appenderClass = it.attributes.getNamedItem("class")?.textContent ?: "",
                        filePatterns = it.childNodes.getChildrenByTagName("file")
                            .map { l -> l.firstChild?.textContent ?: "" }, // An appender may have more than one file, but a file element has only one child file name.
                        maxLevel = maxOf( // Take the max log level of any filter on the appender and the level of the root logger. The filter may not exist, which we model as a default level of "DEBUG"
                            rootLoggerLevel,
                            LogLevel.valueOf(
                                it.childNodes.getChildrenByTagName("filter")
                                    .firstOrNull()
                                    ?.childNodes?.getChildrenByTagName("level")
                                    ?.firstOrNull()
                                    ?.firstChild?.textContent ?: "INFO"
                            )
                        ),
                        encoderPattern = it.childNodes.getChildrenByTagName("encoder")
                            .firstOrNull()
                            ?.childNodes?.getChildrenByTagName("pattern")
                            ?.firstOrNull()
                            ?.firstChild?.textContent ?: "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"
                    )
                }
            return LogSettings(logbackAppenders, logbackSettings)
        }
    }
}