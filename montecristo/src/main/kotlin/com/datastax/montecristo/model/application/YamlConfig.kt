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

package com.datastax.montecristo.model.application

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode

open class YamlConfig(private val basedata: JsonNode) {

    // generic retriever
    fun get(name: String): String {
        return getValueFromPath( name, "")
    }

    // generic retriever with a default
    fun get(name: String, default: String, isList: Boolean = false): String {
        return if (isList) {
            getListValueFromPath( name, default)
        } else {
            getValueFromPath(name, default)
        }
    }

    // Change the json node into a map object - primarily used in comparison code
    fun asMap(): Map<String, Any> {
        val mapper = ObjectMapper()
        return mapper.convertValue(basedata, object : TypeReference<Map<String, Any>>() {})
    }


    fun getValueFromPath(path: String, default: String): String {

        val pathArray = path.split(".")
        var tempNode = basedata
        pathArray.forEach {
            tempNode = tempNode.path(it)
            // if we get back an array node, this indicates we hit a yaml list (denoted by the dash)
            // in the main, we always want the first item and then continue on the path, if the list is wanted, then use getListValueFromJsonPath
            if (tempNode is ArrayNode) {
                tempNode = tempNode[0]
            }
        }
        return if (tempNode.isMissingNode || tempNode.toString().trim() == "" ) {
            default
        } else {
            // need to strip the outer string quotes that the json will of surrounded the YAML value with
            stripChars(tempNode.toString())
        }
    }

    private fun getListValueFromPath(path: String, default: String): String {
        val pathArray = path.split(".")
        var tempNode = basedata
        pathArray.forEach {
            tempNode = tempNode.path(it)
        }
        return if (tempNode.isMissingNode) {
            default
        } else {
            // whether it is a list or not, (could be a single item list, it just has the potential to be a list)
            stripChars(tempNode.toString())

        }
    }

    private fun stripChars(value: String): String {
        // the json contains [ and ] bracketing and the double quotes around all string.
        return value.replace("[", "").replace("\"", "").replace("]", "")
    }

    fun isEmpty(): Boolean {
        return basedata.isNull || basedata.isEmpty
    }
}