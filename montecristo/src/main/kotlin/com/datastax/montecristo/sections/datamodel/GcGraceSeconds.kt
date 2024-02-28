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

class GcGraceSeconds : DocumentSection {

	override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ) : String {

		val md = MarkdownTable("Table", "GC Grace Seconds")
				.orMessage("""
					---
					
					_**Noted for reference:** All tables are using the default value for GC Grace Seconds._
					
					---
					
					""".trimIndent()
				)
		val tablesWithNonDefaultGcGrace = cluster.schema.getUserTables()
				.filter { table -> table.gcGrace != 864000}
				.sortedBy { table -> table.gcGrace }

		tablesWithNonDefaultGcGrace.forEach {
			md.addRow().addField(it.name).addField(it.gcGrace)
		}

		val args = super.createDocArgs(cluster)
		args["gc_grace_values"] = md.toString()

		if (tablesWithNonDefaultGcGrace.count() > 0) {
			recs.immediate(RecommendationType.DATAMODEL,"We recommend evaluating the cause of using non-default gc_grace_seconds.  Using lower than the default can result in data loss under certain conditions.  Using greater than the default can keep tombstones around longer than desired, wasting disk space. Read [this TLP blog post](https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html) for more information.", "We recommend evaluating the cause of using non-default gc_grace_seconds.")
		}

        return compileAndExecute("datamodel/datamodel_gc_grace_seconds.md", args)
	}
}