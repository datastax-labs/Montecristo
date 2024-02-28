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

import java.util.regex.Pattern

class TOCGenerator(private val body: String) {
    fun extractHeaders() :List<Pair<String, Int>>  {

        // Name & pair combo
        val final : MutableList<Pair<String, Int>>  = mutableListOf()
        val pattern = Pattern.compile("^(#+)\\s*(.*)", Pattern.MULTILINE) // anything line with a #
        val matcher = pattern.matcher(removeCodeBlocks(body))
        while(matcher.find()) {
            final.add(Pair(matcher.group(2), matcher.group(1).length))
        }

        return final

    }

    fun getMarkdown() : String {
        val result = StringBuilder()
        for(heading in extractHeaders()) {
            val toReplace = Regex("""[\s/]""")
            val link = heading.first.replace(toReplace, "-").toLowerCase()
            val line = "    ".repeat(heading.second - 1) + "* [" + heading.first + "](#$link)"
            result.appendLine(line)
        }


        return result.toString()
    }


    companion object {
        fun removeCodeBlocks(text: String): String {
            val pattern = Pattern.compile("(```[a-z]*\\n[\\s\\S]*?\\n```)", Pattern.MULTILINE) // markdown code blocks
            val matcher = pattern.matcher(text)
            return matcher.replaceAll("")
        }
    }

}