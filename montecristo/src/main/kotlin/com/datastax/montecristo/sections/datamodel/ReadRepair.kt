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

package com.datastax.montecristo.sections.datamodel

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.immediate
import com.datastax.montecristo.utils.MarkdownTable

class ReadRepair : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {

        val args = super.createDocArgs(cluster)

        if (!cluster.databaseVersion.supportsReadRepair()) {
            return ""
        }

        val nonZeroRrTables = cluster.schema.tables
            .filter { table -> table.readRepair.toDoubleOrNull() ?: 0.0  != 0.0 || table.dcLocalReadRepair.toDoubleOrNull()?: 0.1  != 0.0 }

        var tablesUsingGlobalRR = 0
        var tablesUsingDCRR = 0

        val md = MarkdownTable("Table", "DC Local Read Repair", "Global Read Repair").orMessage("No tables are using read repair.")
        nonZeroRrTables.forEach {
            md.addRow().addField(it.name).addField(it.dcLocalReadRepair).addField(it.readRepair)
            if (it.readRepair.toDoubleOrNull() ?: 0.0 > 0.0) {
                tablesUsingGlobalRR++
            }
            if (it.dcLocalReadRepair.toDoubleOrNull()?: 0.1  > 0.0) {
                tablesUsingDCRR++
            }
        }

        // todo show different message in single vs multi-dc environments
        if (tablesUsingGlobalRR > 0) {
            recs.immediate(RecommendationType.DATAMODEL,"Tables are using global (multi-dc) read repair which can add significant overhead. We recommend setting read_repair_chance to zero on all tables.")
        }


        if (tablesUsingDCRR > 0) {
            recs.immediate(RecommendationType.DATAMODEL,"Tables are using dc local read repair. We recommend setting dclocal_read_repair_chance to zero on all tables.")
        }

        args["repairTable"] = md.toString()

        return compileAndExecute("datamodel/datamodel_readrepair.md", args)
    }

}
