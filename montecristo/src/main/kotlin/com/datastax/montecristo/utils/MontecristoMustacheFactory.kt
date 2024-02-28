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

import com.github.mustachejava.DefaultMustacheFactory
import java.io.StringReader
import java.io.StringWriter
import java.io.Writer

class MontecristoMustacheFactory : DefaultMustacheFactory() {
    // this is overridden to stop the HTML escape characters
    override fun encode(value: String?, writer: Writer?) {
        writer!!.write(value)
    }

    fun compileAndExecute(resource: String, scope: Any): String {
        val result = StringWriter()
        val input = StringReader(this::class.java.getResource("/templates/$resource").readText())
        val template = compile(input, resource)
        template.execute(result, scope)
        return result.toString()
    }


}