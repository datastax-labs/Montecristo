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

package com.datastax.montecristo.metrics

import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.metrics.SSTableCount
import com.datastax.montecristo.model.metrics.Server
import com.datastax.montecristo.model.metrics.ServerMetricList

/*
* This has been pulled into an interface, so that for a normal tarball, we may use the Dao / sql lite implementation to provide the functionality
* but in tests we can implement a different source, without the need for mock or running into actually creating a DB behind the methods
 */
interface IMetricServer {

    fun getHistogram(keyspace: String, table: String, histogram: String, key: String) : ServerMetricList

    fun getUnusedKeyspaces(): List<String>

    fun getDroppedCounts(name: String) : Map<String, Long>

    fun getBlockedTaskCounts(name: String) : Map<String, Long>

    fun getSStableCounts(overThreshold: Int = 0) : List<SSTableCount>

    fun getRuntimeVariables(node: Node) : Map<String, String>

    fun getServers(): List<Server>

    fun getSchemaVersions() : Map<String,String>

    fun getLogDurations() : Map<String, Pair<String,String>>

    fun getLogStats() : Map<String, Pair<String,String>>

    fun getClientRequestMetrics() : List<NodeTableReadCount>

}