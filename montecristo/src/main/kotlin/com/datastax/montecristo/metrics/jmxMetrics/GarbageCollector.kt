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

package com.datastax.montecristo.metrics.jmxMetrics

import java.sql.PreparedStatement

class GarbageCollector(val node: String,
                       val type: String,
                       val name: String,
                       private val pkey: String,
                       val key: String,
                       val value: String) : JMXMetric {
    override fun getPrepared(): String {
        return "INSERT INTO garbage_collector (node, type, name, pkey, key, value) VALUES (?,?,?,?,?,?)"
    }

    override fun bindAll(preparedStatement: PreparedStatement) {
        preparedStatement.setString(1, node)
        preparedStatement.setString(2, type)
        preparedStatement.setString(3, name)
        preparedStatement.setString(4, pkey)
        preparedStatement.setString(5, key)
        preparedStatement.setString(6, value)
    }
    override fun toString() : String {
        return StringBuilder()
            .append("INSERT INTO garbage_collector (node, type, name, pkey, key, value) VALUES (")
            .append("$node,")
            .append("$type,")
            .append("$name,")
            .append("$pkey,")
            .append("$key,")
            .append("$value,")
            .append(")")
            .toString()
    }
}