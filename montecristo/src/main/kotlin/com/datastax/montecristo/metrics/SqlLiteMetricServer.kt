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

package com.datastax.montecristo.metrics

import com.github.andrewoma.kwery.core.DefaultSession
import com.github.andrewoma.kwery.core.dialect.SqliteDialect
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.metrics.SSTableCount
import com.datastax.montecristo.model.metrics.Server
import com.datastax.montecristo.model.metrics.ServerMetricList
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement


class SqlLiteMetricServer(sqliteDb: String, loadSchema: Boolean = false) : IMetricServer {
    val session: DefaultSession
    val connection: Connection

    private lateinit var insertServerQuery: PreparedStatement
    private lateinit var getHistogramQuery: PreparedStatement
    private lateinit var getDroppedMessageQuery: PreparedStatement
    private lateinit var getBlockedTasks: PreparedStatement
    private lateinit var getSSTableCounts: PreparedStatement
    lateinit var getRuntime: PreparedStatement
    private lateinit var getSchemaVersionsQuery: PreparedStatement
    private lateinit var insertDBQuery: PreparedStatement
    private lateinit var retrieveLogMinMax: PreparedStatement
    private lateinit var retrieveLogStats: PreparedStatement
    private lateinit var getClientRequestMetrics : PreparedStatement

    init {
        Class.forName("org.sqlite.JDBC")
        connection = DriverManager.getConnection("jdbc:sqlite:$sqliteDb")
        session = DefaultSession(connection, SqliteDialect())
        if (loadSchema)
            loadSchema()
        prepareAll()
    }


    private fun loadSchema() {
        val queries = this::class.java.getResource("/setup.sql")?.readText() ?: ""
        val stmt = connection.createStatement()
        stmt.executeUpdate(queries)
    }


    private fun prepareAll() {
        insertServerQuery = connection.prepareStatement("INSERT INTO servers (host, memory_in_gb, aws_region, aws_instance_type, has_jmx) VALUES (?, ?, ?, ?, ?)")

        getHistogramQuery = connection.prepareStatement("""select node, value from tablehistograms
                                    |where keyspace = ?
                                    |and tablename = ?
                                    |and name = ?
                                    |and key = ?""".trimMargin())


        // scope here is something like BINARY or MUTATION
        getDroppedMessageQuery = connection.prepareStatement("""select node, value
                                                                | FROM metrics where type = 'DroppedMessage'
                                                                | AND key = 'Count'
                                                                | AND scope = ?
                                                            """.trimMargin())

        getBlockedTasks = connection.prepareStatement("""SELECT node, value
            |FROM internal WHERE key = 'TotalBlockedTasks' AND type = ?
        """.trimMargin())

        getSSTableCounts = connection.prepareStatement("""select node, keyspace, tablename, value
                                                                | FROM tableHistograms
                                                                | WHERE name = 'LiveSSTableCount'
                                                            """.trimMargin())

        getRuntime = connection.prepareStatement("select key,value from db where node = ? and keyspace = '' and tablename = ''")

        getSchemaVersionsQuery = connection.prepareStatement("select node, value from db where key like 'SchemaVersion'")

        insertDBQuery = connection.prepareStatement("INSERT INTO db (node, keyspace, tablename, key, value) VALUES (?, ?, ?, ?, ?)")

        retrieveLogMinMax = connection.prepareStatement("select node " +
                ",max(case key when 'minLogDate' then value else null end) as minLogDate " +
                ",max(case key when 'maxLogDate' then value else null end) as maxLogDate " +
                "from db where key = 'minLogDate' or key = 'maxLogDate' " +
                "group by node")

        retrieveLogStats = connection.prepareStatement("select node " +
                ",sum(case key when 'valid_entries' then value else null end) as valid_entries " +
                ",sum(case key when 'invalid_entries' then value else null end) as invalid_entries " +
                "from db where keyspace = 'log_stats' " +
                "group by node")

        getClientRequestMetrics = connection.prepareStatement("select node, keyspace, tablename, value from tablehistograms " +
                "where name = 'CoordinatorReadLatency' and key = 'Count'")

    }

    /**
     * key is something like 99thPercentile
     */
    override fun getHistogram(keyspace: String, table: String, histogram: String, key: String): ServerMetricList {
        val result = ServerMetricList(mutableListOf())
        getHistogramQuery.setString(1, keyspace)
        getHistogramQuery.setString(2, table)
        getHistogramQuery.setString(3, histogram)
        getHistogramQuery.setString(4, key)
        val data = getHistogramQuery.executeQuery()

        while (data.next()) {
            val node = data.getString(1)
            val value = data.getDouble(2)
            result.add(Pair(node, value))
        }
        return result
    }


    override fun getUnusedKeyspaces(): List<String> {

        val query = """SELECT a.keyspace, sum(CAST(a.value as double) + CAST(b.value as double)) as writes
          		     FROM tablehistograms a, tablehistograms b
                   WHERE a.name = 'WriteLatency' 
                     AND a.key='Count' and a.keyspace != '' and a.keyspace not like 'system%' 
                     AND a.tablename != ''
                     and a.tablename = b.tablename
                     and a.keyspace = b.keyspace
                     and a.node = b.node
                     and b.name = 'ReadLatency'
                   GROUP BY 1
                   HAVING sum(CAST(a.value as double) + CAST(b.value as double)) = 0
                   """

        return session.select(query) { row -> row.string("keyspace") }
    }

    /**
     * returns the dropped counts per server, internally it's own as the scope
     */
    override fun getDroppedCounts(name: String): Map<String, Long> {
        getDroppedMessageQuery.setString(1, name)
        return getSimpleValueMap(getDroppedMessageQuery)
    }

    override fun getBlockedTaskCounts(name: String): Map<String, Long> {
        getBlockedTasks.setString(1, name)
        return getSimpleValueMap(getBlockedTasks)
    }

    private fun getSimpleValueMap(statement : PreparedStatement) : Map<String,Long> {
        val data = statement.executeQuery()
        val result = mutableMapOf<String, Long>()
        while (data.next()) {
            result[data.getString("node")] = data.getLong("value")
        }
        return result
    }

    // function takes a threshold so that we have the option of getting all of the sstable counts, or just the ones
    // with high values.
    override fun getSStableCounts(overThreshold: Int): List<SSTableCount> {
        val data = getSSTableCounts.executeQuery()

        val result = mutableListOf<SSTableCount>()
        while (data.next()) {
            // filtering vs threshold is done here, the data type of value is text, and converting it all in the query
            // isn't going to go well in this instance.
            if (data.getInt("value") > overThreshold) {
                result.add(SSTableCount(data.getString("node"), data.getString("keyspace"), data.getString("tablename"), data.getInt("value")))
            }
        }
        return result
    }

    override fun getRuntimeVariables(node: Node): Map<String, String> {
        getRuntime.setString(1, node.hostname)
        val data = getRuntime.executeQuery()

        val result = mutableMapOf<String, String>()
        while (data.next()) {
            val key = data.getString(1)
            val value = data.getString(2)
            result[key] = value
        }
        return result

    }


    fun insertServer(hostname: String, memory_in_gb: Int, aws_region: String, aws_instance_type: String, hasJmx: Boolean) {
        insertServerQuery.setString(1, hostname)
        insertServerQuery.setInt(2, memory_in_gb)
        insertServerQuery.setString(3, aws_region)
        insertServerQuery.setString(4, aws_instance_type)
        insertServerQuery.setBoolean(5, hasJmx)
        insertServerQuery.executeUpdate()
    }

    fun insertDBValues(node: String, keyspace: String, tablename: String, key: String, value: String) {
        insertDBQuery.setString(1, node)
        insertDBQuery.setString(2, keyspace)
        insertDBQuery.setString(3, tablename)
        insertDBQuery.setString(4, key)
        insertDBQuery.setString(5, value)
        insertDBQuery.executeUpdate()
    }

    /*
    returns a list of all servers and their meta (server table full table scan)
     */
    override fun getServers(): List<Server> {
        val result = mutableListOf<Server>()
        session.select("SELECT * from servers") { row ->
            Server(row.string("host"), row.int("memory_in_gb"), row.string("aws_region"), row.string("aws_instance_type"), row.boolean("has_jmx"))
        }.forEach { result.add(it) }
        return result
    }

    override fun getSchemaVersions(): Map<String, String> {
        val data = getSchemaVersionsQuery.executeQuery()
        val result = mutableMapOf<String, String>()

        while (data.next()) {
            val key = data.getString("node")
            val value = data.getString("value")
            result[key] = value
        }
        return result
    }

    override fun getLogDurations(): Map<String, Pair<String, String>> {
        val data = retrieveLogMinMax.executeQuery()
        val result = mutableMapOf<String, Pair<String, String>>()

        while (data.next()) {
            val node = data.getString("node")
            val minLogDate = data.getString("minLogDate")
            val maxLogDate = data.getString("maxLogDate")
            result[node] = Pair(minLogDate, maxLogDate)
        }
        return result
    }

    override fun getLogStats(): Map<String, Pair<String, String>> {
        val data = retrieveLogStats.executeQuery()
        val result = mutableMapOf<String, Pair<String, String>>()

        while (data.next()) {
            val node = data.getString("node")
            val validEntries = data.getString("valid_entries")
            val invalidEntries = data.getString("invalid_entries")
            result[node] = Pair(validEntries, invalidEntries)
        }
        return result
    }


    override fun getClientRequestMetrics(): List<NodeTableReadCount> {
        val data = getClientRequestMetrics.executeQuery()
        val result = mutableListOf<NodeTableReadCount>()

        while (data.next()) {
            val node = data.getString("node")
            val keyspace = data.getString("keyspace")
            val tableName = data.getString("tablename")
            val count = data.getLong("value")
            result.add(NodeTableReadCount(node, keyspace, tableName, count))
        }
        return result
    }
}