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

package com.datastax.montecristo.utils

import java.util.*

class MarkdownRow(var table: MarkdownTable) {
    var fields = mutableListOf<String>()

    /**
     * You can literally pass anything to the markdown table and it'll render.
     */
    fun addField(value: Any) : MarkdownRow {
        fields.add(value.toString())
        return this
    }

    fun addBlankRow(numberOfFields : Int) : MarkdownRow {
        fields.addAll(MutableList(numberOfFields) { "" })
        return this
    }

    override fun toString(): String {
        return fields.joinToString("|")
    }
}

class MarkdownTable(vararg var headers: String) {
    var rows = mutableListOf<MarkdownRow>()
    var message: String = ""

    fun orMessage(s: String) : MarkdownTable {
        message = s
        return this
    }

    fun addRow() : MarkdownRow {
        val row = MarkdownRow(this)
        rows.add(row)
        return row
    }

    override fun toString() : String {
        if(rows.isEmpty()) return message

        val result = StringBuilder()
        val h = headers.joinToString("|") { "<span><div style=\"text-align:left;\">$it</div></span>" }
        val sep = Collections.nCopies(headers.size, "---").joinToString("|")

        result.appendLine(h)
        result.appendLine(sep)

        rows.forEach { result.appendLine(it.toString()) }
        return result.toString()
    }
}