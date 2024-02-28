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

// TODO - why is options a list of pairs and not a map, can be duplicates?
data class Keyspace(val name : String, val options : List<Pair<String,Int>>, val strategy : String) {

    // helpers
    fun getDCSettings(dc: String) : Int {
        for(r in options) {
            if(r.first == dc)
                return r.second
        }
        return 0
    }

    // TODO - this doesn't look right, the keyspace has its replication settings but we are not parsing them to answer the question
    // we'll assume we ARE in a DC unless NTS explicitly says no
    fun isInDC(name: String) : Boolean {
        // this is a bit weird - we don't really know if the data exists in this keyspace without looking at the tokens
        if(strategy != "NetworkTopologyStrategy") {
            return true
        }
        val settings = getDCSettings(name)
        return settings > 0
    }

    fun isInDCList(names: List<String>): Boolean {
        for(n in names) {
            if(isInDC(n)) return true
        }
        return false
    }

    fun getStrategyShortName() : String {
        return strategy.replace("org.apache.cassandra.locator.","")
    }
}