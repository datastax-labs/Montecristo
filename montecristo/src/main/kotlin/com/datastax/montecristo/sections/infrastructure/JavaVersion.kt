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
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class JavaVersion : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val version =  cluster.nodes.first().osConfiguration.javaVersion
        var javaVersion = ""
        if(version.startsWith("1.7")) {
            recs.immediate(RecommendationType.INFRASTRUCTURE,"Upgrade to Java 8.")
        } else {
            javaVersion = getJavaVersion(version.split("\n").first()) ?: "unknown"
            if (javaVersion.contains("_") && javaVersion.split("_")[1].toInt() < 191) {
                recs.immediate(RecommendationType.INFRASTRUCTURE,"The Java 8 version in use (${javaVersion}) is outdated and should be upgraded for stability and performance improvements. The latest OpenJDK 8 release should be used instead.")
            }
        }
        args ["java-version"] = javaVersion
        args["java-version-output"] = version
        return compileAndExecute("infrastructure/infrastructure_java_version.md", args)
    }

    fun getJavaVersion(line: String): String? {
        val pattern = Regex("""[a-zA-Z]+ version "(.*)"""")
        return pattern.find(line)?.groups?.get(1)?.value
    }

}