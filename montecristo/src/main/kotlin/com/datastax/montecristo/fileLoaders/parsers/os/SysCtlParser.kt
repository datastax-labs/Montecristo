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

package com.datastax.montecristo.fileLoaders.parsers.os

import com.datastax.montecristo.fileLoaders.parsers.IFileParser
import com.datastax.montecristo.model.os.Sysctl

object SysCtlParser : IFileParser<Sysctl> {

    override fun parse(data: List<String>): Sysctl {

        val settingsMap = data.map { it.split("\\s+=\\s+".toRegex()) }
            .filter { it.size == 2 }.associate { Pair(it[0], it[1]) }
        return Sysctl(settingsMap)
    }
}