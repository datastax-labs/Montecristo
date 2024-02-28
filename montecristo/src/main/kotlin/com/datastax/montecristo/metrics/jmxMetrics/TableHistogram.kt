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


class TableHistogram(val node: String,
                     val keyspace: String,
                     val table: String,
                     var name: String,
                     var key: String,
                     var value: String) : JMXMetric {

    override fun bindAll(preparedStatement: PreparedStatement) {
        preparedStatement.setString(1, node)
        preparedStatement.setString(2, keyspace)
        preparedStatement.setString(3, table)
        preparedStatement.setString(4, name)
        preparedStatement.setString(5, key)
        preparedStatement.setString(6, value)
    }

    override fun getPrepared(): String {
        return "INSERT into tablehistograms (node, keyspace, tablename, name, key, value) VALUES (?,?,?,?,?,?)"
    }

    override fun toString() : String {
        return StringBuilder()
            .append("INSERT into tablehistograms (node, keyspace, tablename, name, key, value) VALUES (")
            .append("$node,")
            .append("$keyspace,")
            .append("$table,")
            .append("$name,")
            .append("$key,")
            .append("$value,")
            .append(")")
            .toString()
    }
}
