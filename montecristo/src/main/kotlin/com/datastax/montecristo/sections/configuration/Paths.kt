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

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation

class Paths : DocumentSection {

    private val customParts = StringBuilder()

    /**
    Check if authentication and authorization are enabled.
    Recommend enabling them if they're not.
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val dataDirectories = cluster.getSetting("data_file_directories", ConfigSource.CASS, "/var/lib/cassandra/data", true)
        val commitLogDirectory = cluster.getSetting("commitlog_directory", ConfigSource.CASS, "/var/lib/cassandra/commitlog", true)

        if (dataDirectories.isConsistent()) {
            customParts.append("Data directories in use are: \n\n")
            customParts.append("```\n")
            val dataDirectoriesList = dataDirectories.getSingleValue()
            customParts.append("data_file_directories: \n")
            dataDirectoriesList.split(",").forEach {
                customParts.append("  - ${it}\n")
            }
            customParts.append("```\n\n")
        } else {
            customParts.append("Data directories in use are inconsistent across the cluster\n\n")
            customParts.append("```\n")
            customParts.append(Utils.displayInconsistentConfig(dataDirectories.values.entries))
            customParts.append("```\n\n")
        }

        customParts.append(Utils.formatCassandraYamlSetting(commitLogDirectory))

        args["paths"] = customParts.toString()
        return compileAndExecute("configuration/configuration_paths.md", args)
    }
}
