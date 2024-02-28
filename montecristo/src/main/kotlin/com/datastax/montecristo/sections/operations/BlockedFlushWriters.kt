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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MarkdownTable

class BlockedFlushWriters : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        val blockedTable = MarkdownTable("Node", "Blocked Flush Writers").orMessage("There were no blocked flush writers found on the nodes.")

        for (row in cluster.blockedTasks.memtableFlushWriters) {
            blockedTable.addRow().addField(row.key).addField(row.value)

        }
        args["blockedTable"] = blockedTable.toString()

        val setting = cluster.getSetting("memtable_flush_writers", ConfigSource.CASS, "")

        if (setting.areAllUnset()) {
            // not set anywhere
            args["blockedFlushWritersSetting"] = "Currently `memtable_flush_writers` is not set in configuration, which defaults to one flush writer per data directory."
        } else {
            // set, universally
            args["blockedFlushWritersSetting"] = Utils.formatCassandraYamlSetting(setting)
        }

        return compileAndExecute("operations/operations_blocked_flush_writers.md", args)
    }

}