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
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate

class TrickleFsync : DocumentSection {

    private val customParts = StringBuilder()

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        val trickleFsync = cluster.getSetting("trickle_fsync", ConfigSource.CASS, "false")

        customParts.append(Utils.formatCassandraYamlSetting(trickleFsync))
        args["trickle"] = customParts.toString()

        if (cluster.nodes.any { node ->
                val dataDeviceName = node.storage.storageLocations.dataLocation()
                val rotational = node.storage.lsBlk[dataDeviceName]?.isRotational == true // if we do not know, we are going to assume we should recommend
                node.cassandraYaml.get("trickle_fsync", "false", false) == "false"
                    && !rotational
        } ) {
            recs.immediate(RecommendationType.CONFIGURATION, "We recommend that all nodes with SSD / M2.NVMe storage should have the trickle_fsync value set to `true`")
        }

        return compileAndExecute("configuration/configuration_trickle_fsync.md", args)
    }
}
