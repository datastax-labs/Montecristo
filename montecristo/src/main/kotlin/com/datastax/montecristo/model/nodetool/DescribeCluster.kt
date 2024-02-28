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

package com.datastax.montecristo.model.nodetool

data class DescribeCluster (val data : List<String>, val path : String) {

    fun getName() : String {
        val name = data.firstOrNull { it.contains("Name:") }
        return if (name.isNullOrBlank()) {
            return "UNKNOWN"
        } else {
            name.split(":")[1].trim()
        }
    }

    fun getSnitch() : String {
        val snitch =  data.firstOrNull { it.contains("Snitch:") }
        return if (snitch.isNullOrBlank()) {
            return "UNKNOWN"
        } else {
            snitch.split(":")[1].trim()
        }
    }

    fun getPartitioner() : String {
        val partitioner =  data.firstOrNull { it.contains("Partitioner:") }
        return if (partitioner.isNullOrBlank()) {
            return "UNKNOWN"
        } else {
            partitioner.split(":")[1].trim()
        }
    }
}
