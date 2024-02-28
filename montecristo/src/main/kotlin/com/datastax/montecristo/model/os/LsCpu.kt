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

package com.datastax.montecristo.model.os

class LsCpu(private val info: Map<String, String?>) {

    fun get(name: String): String? {
        return info[name]
    }

    // helpers
    /*
      This is listed here as CPU threads because the terminology is a little ambiguous.
      CPUs is vague - it could mean physical, cores, or threads
      So 2 physical CPUs, each with 2 cores and 2 hyper threads = 8 cpuThreads
   */
    fun getCpuThreads() : Int {
        return when {
            info.keys.contains("CPUs") -> {
                get("CPUs")!!.toInt()
            }
            info.keys.contains("CPU(s)") -> {
                get("CPU(s)")!!.toInt()
            }
            else -> {
                0
            }
        }
    }

    val sockets get() = get("Sockets")?.toInt()
    // This doesn't appear if we're not virtualized
    val hypervisorVendor get() = get("Hypervisor vendor")

    fun getModelName() : String?
    {
        return when {
            info.containsKey("Model name") -> {
                info["Model name"]
            }
            info.containsKey("Model") -> {
                info["Model"]
            }
            else -> {
                ""
            }
        }
    }
}