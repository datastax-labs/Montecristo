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

package com.datastax.montecristo.model.metrics

import com.datastax.montecristo.metrics.IMetricServer

data class BlockedTasks(val metricsServer: IMetricServer) {
    val antiEntropy =  metricsServer.getBlockedTaskCounts( "AntiEntropyStage")
    val compaction = metricsServer.getBlockedTaskCounts("CompactionExecutor")
    val gossip = metricsServer.getBlockedTaskCounts( "GossipStage")
    val hintedHandoff = metricsServer.getBlockedTaskCounts( "HintedHandoff")
    val memtableFlushWriters = metricsServer.getBlockedTaskCounts( "MemtableFlushWriter")
    val migration = metricsServer.getBlockedTaskCounts( "MigrationStage")
    val validation = metricsServer.getBlockedTaskCounts("ValidationExecutor")
}

