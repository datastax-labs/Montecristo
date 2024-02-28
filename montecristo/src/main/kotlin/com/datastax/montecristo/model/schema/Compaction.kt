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

package com.datastax.montecristo.model.schema

data class CompactionDetail(val name: String, val shortName: String, val options: Map<String, String>) {

    // These are short cut creators for the object
    companion object {

        fun stcs(params: Map<String, String> = mutableMapOf()) : CompactionDetail {
            return CompactionDetail("SizeTieredCompactionStrategy", "STCS", params)
        }
        fun lcs(params: Map<String, String> = mutableMapOf()) : CompactionDetail {
            return CompactionDetail("LeveledCompactionStrategy", "LCS", params)
        }
        fun twcs(params: Map<String, String> = mutableMapOf()) : CompactionDetail {
            return CompactionDetail("TimeWindowCompactionStrategy", "TWCS", params)
        }
        fun dtcs(params: Map<String, String> = mutableMapOf()) : CompactionDetail {
            return CompactionDetail("DateTieredCompactionStrategy", "DTCS", params)
        }
        fun cfs(params: Map<String, String> = mutableMapOf()) : CompactionDetail {
            return CompactionDetail("com.datastax.bdp.hadoop.cfs.compaction.CFSCompactionStrategy", "CFS", params)
        }
        fun none() : CompactionDetail {
            return CompactionDetail("NONE", "NONE", mapOf())
        }

    }

    fun getOption(option: String, default : String) : String {
        return options.getOrDefault(option, default)
    }
}