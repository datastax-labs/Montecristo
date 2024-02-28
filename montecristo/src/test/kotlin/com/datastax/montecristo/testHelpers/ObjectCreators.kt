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

package com.datastax.montecristo.testHelpers

import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.Workload
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.DseYaml
import com.datastax.montecristo.model.application.JvmSettings
import com.datastax.montecristo.model.application.SolrConfig
import com.datastax.montecristo.model.logs.LogSettings
import com.datastax.montecristo.model.logs.LogbackAppender
import com.datastax.montecristo.model.metrics.SSTableStatistics
import com.datastax.montecristo.model.nodetool.*
import com.datastax.montecristo.model.os.Configuration
import com.datastax.montecristo.model.storage.Storage
import com.datastax.montecristo.model.versions.DatabaseVersion
import io.mockk.mockk

object ObjectCreators {

    // Node creation, optional parameters for all, so you can just send in what you need.
    fun createNode(nodeName: String = "default_node_name",
                   listenAddress: String = "10.1.1.1",
                   cassandraYaml: CassandraYaml = mockk<CassandraYaml>(relaxed = true),
                   config: Configuration = mockk<Configuration>(relaxed = true),
                   ring : List<Ring> = mockk<List<Ring>>(relaxed=true),
                   status: Status = mockk<Status>(relaxed = true),
                   tpStats: TpStats = mockk<TpStats>(relaxed = true),
                   info: Info = mockk<Info>(relaxed = true),
                   describeCluster: DescribeCluster = mockk<DescribeCluster>(relaxed = true),
                   dseYaml: DseYaml = mockk<DseYaml>(relaxed = true),
                   jvmSettings: JvmSettings = mockk<JvmSettings>(relaxed = true),
                   logSettings: LogSettings = LogSettings(emptyList(), emptyList()) ,
                   workload :  List<Workload> = listOf(Workload.CASSANDRA),
                   gossipInfo : Map<String,GossipInfo> = emptyMap(),
                   isDse : Boolean = false,
                   databaseVersion : DatabaseVersion = DatabaseVersion.fromString ("3.11.13"),
                   ssTableStatistics: SSTableStatistics = SSTableStatistics(emptyList()),
                   solrConfig: Map<String,SolrConfig> = emptyMap(),
                   storage : Storage = mockk<Storage>(relaxed = true)
    ): Node {


        return Node(nodeName, config, storage, status, ring, tpStats, info, describeCluster, listenAddress, cassandraYaml, dseYaml, jvmSettings, logSettings, workload, gossipInfo,isDse,databaseVersion,ssTableStatistics, solrConfig)

    }
}