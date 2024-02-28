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

package com.datastax.montecristo.sections

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MontecristoMustacheFactory

interface DocumentSection {

	fun getDocument(
		cluster: Cluster,
		logSearcher: Searcher,
		recs: MutableList<Recommendation>,
		executionProfile: ExecutionProfile
	): String

	fun compileAndExecute(template: String, scope: Any) : String {
		val mf = MontecristoMustacheFactory()
		return mf.compileAndExecute(template, scope)
	}

	fun createDocArgs(cluster : Cluster) : MutableMap<String,Any> {
		val args = mutableMapOf<String, Any>()
		args["software"] = if (cluster.isDse) { "DSE" } else { "Cassandra" }
		return args
	}

	fun processValueWithColour(value : String, color : String) : String {
		return if (color.isNotEmpty()) {
			"<span style=\"color:$color;\">$value</span>"
		} else {
			value
		}
	}

	fun String.plural(pluralString : String, count : Int): String? {
		return if (count > 1) {
			pluralString
		} else {
			this
		}
	}
}