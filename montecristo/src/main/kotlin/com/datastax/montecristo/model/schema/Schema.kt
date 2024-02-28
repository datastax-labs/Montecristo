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

// TODO - handle index class, udt class etc
data class Schema(val keyspaces : List<Keyspace>,
                  val tables : List<Table>,
                  val secondaryIndexes : List<Table>,
                  val saiIndexes : List<Table>,
                  val sasiIndexes : List<Table>,
                  val searchIndexes : List<Table>,
                  val types : List<UserDefinedType>,
                  val functions : Map<String,String>,
                  val isSchemaInAgreement : Boolean) {


    private val systemKeyspaces = listOf("OpsCenter","system","system_schema","system_traces", "system_distributed", "system_auth", "dse_insights","dse_security","dse_leases","dse_system","dse_system_local","dse_perf", "solr_admin", "reaper_db", "dse_insights", "dse_insights_local", "dsefs", "dse_analytics", "spark_system", "HiveMetaStore", "cfs", "cfs_archive")

    // helpers
    fun getUserKeyspaces() : List<Keyspace> {
        return keyspaces.filter { !systemKeyspaces.contains(it.name) }
    }
    fun getUserTables() : List<Table> {
        return tables.filter { !systemKeyspaces.contains(it.getKeyspace()) }
    }

    fun getUserMaterializedViews() : List<Table> {
        return tables.filter { it.isMV && !systemKeyspaces.contains(it.getKeyspace()) }
    }

    fun getUserSAIIndexes() : List<Table> {
        return saiIndexes.filter { !systemKeyspaces.contains(it.getKeyspace()) }
    }

    fun getUserIndexes() : List<Table> {
        return secondaryIndexes.filter { !systemKeyspaces.contains(it.getKeyspace()) }
    }

    fun getUnusedUserTables(): List<Table> {
        return getUserTables().filter {
            it.readLatency.count.sum().toLong() == 0.toLong() && it.writeLatency.count.sum().toLong() == 0.toLong()
        }
    }

    fun getKeyspace(name: String): Keyspace? {
        for (k in keyspaces) {
            if (k.name == name)
                return k
        }
        return null
    }

    fun getTable(name: String): Table? {
        for (t in tables) {
            if (t.name == name)
                return t
        }
        return null
    }

    fun getUserTypes() : List<String> {
        return types.filter { !isInSystemKeyspace(it.name,"") }.map { it.rawStatement }
    }

    fun getUserFunctions() : Map<String,String> {
        return functions.filter { !isInSystemKeyspace(it.key, "CREATE FUNCTION") }
    }

    private fun isInSystemKeyspace(name : String, prefix : String) : Boolean {
        return if (name.contains(".")) {
            val parsedString = name.replace(prefix,"").trim()
            systemKeyspaces.contains(parsedString.split(".")[0])
        } else {
            false
        }
    }

}
