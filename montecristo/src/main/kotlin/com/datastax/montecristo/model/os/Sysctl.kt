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

data class Sysctl(val info: Map<String, String>) {

    fun get(name: String): String? {
        return info[name]
    }

    // helpers
    val vmswappiness get() = get("vm.swappiness")
    val vmMaxMapCount get() = get("vm.max_map_count")

    val netRmemMax get() = get("net.core.rmem_max")
    val netWmemMax get() = get("net.core.wmem_max")
    val netRmemDefault get() = get("net.core.rmem_default")
    val netWmemDefault get() = get("net.core.wmem_default")
    val netOptMemMax get() = get ("net.core.optmem_max")
    val netIpv4TcpRmem get() = get ("net.ipv4.tcp_rmem")
    val netIpv4TcpWmem get() = get ("net.ipv4.tcp_wmem")
}