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
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class Logging : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val customParts = StringBuilder()
        val args = super.createDocArgs(cluster)

        val logbackSettings = cluster.nodes.first().logSettings

        if (logbackSettings.appenders.isNotEmpty()) {
            customParts.append(logbackSettings.rawLogBackSettings.joinToString ("\n"))
            logbackSettings.appenders
                .filter { appender ->
                    appender.name == "ASYNCDEBUGLOG" || appender.name == "STDOUT"
                }
                .forEach{ line -> run{
                        if (line.name == "STDOUT") {
                            recs.immediate(RecommendationType.CONFIGURATION,disableStdoutLogging)
                        }
                    if (line.name == "ASYNCDEBUGLOG" && cluster.databaseVersion.shouldDisableDebugLogging()) {
                            recs.immediate(RecommendationType.CONFIGURATION,disableDebugLogging)
                        }
                    }
                }
        }

        // strip the comments out, we don't need them
        val stripped = customParts.toString()
                .replace("\\s*#.*".toRegex(), "")
                .replace("\n+".toRegex(), "\n")


        args["loggingConfig"] = stripped

        return compileAndExecute("configuration/configuration_logging.md", args)

    }


    private val disableStdoutLogging = """
            We recommend switching off STDOUT logging as it is unmonitored and has a performance impact.
            In the file above, this can be done by removing the following line: `<appender-ref ref="STDOUT" />`
        """.trimIndent().replace('\n', ' ')
    private val disableDebugLogging = """
            We recommend switching off ASYNCDEBUGLOG logging as it has a performance impact.
            In the file above, this can be done by removing the following line: `<appender-ref ref="ASYNCDEBUGLOG" />`
        """.trimIndent().replace('\n', ' ')
}