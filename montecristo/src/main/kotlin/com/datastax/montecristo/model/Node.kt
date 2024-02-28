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

package com.datastax.montecristo.model

import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.DseYaml
import com.datastax.montecristo.model.application.JvmSettings
import com.datastax.montecristo.model.application.SolrConfig
import com.datastax.montecristo.model.logs.LogSettings
import com.datastax.montecristo.model.metrics.SSTableStatistics
import com.datastax.montecristo.model.nodetool.*
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.storage.Storage
import com.datastax.montecristo.model.versions.DatabaseVersion


/**
 * Container for a cassandra node-centric view of itself
 * Should be able to get everything from here
 * this should be the primary object in which everything is loaded
 * all other views are shortcuts
 * Abstractly a node doesn't have to be equal to a physical server
 * We could run multiple nodes on a server, and will in the future
 *
 * nodetool status is a FULL view of the status of the cluster from the perspective of this node
 * extracting all the individual gossip parts is going to require improving the metrics parser
 * because it's multi-line
 */

data class Node(val hostname: String,
                val osConfiguration: Configuration,
                val storage: Storage,
                val status: Status,
                val ring: List<Ring>,
                val tpStats: TpStats,
                val info: Info,
                val describeCluster : DescribeCluster,
                val listenAddress: String,
                val cassandraYaml: CassandraYaml,
                val dseYaml: DseYaml,
                val jvmSettings: JvmSettings,
                val logSettings: LogSettings,
                val workload: List<Workload>,
                val gossipInfo: Map<String, GossipInfo>,
                val isDse : Boolean,
                val databaseVersion: DatabaseVersion,
                val ssTableStats : SSTableStatistics,
                val solrConfig : Map<String, SolrConfig>) {

    override fun toString(): String {
        return hostname
    }

    val workloads: String
        get() {
            return workload.sortedBy { it.name }.joinToString(",") { it.toString().capitalize() }
        }
}