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

data class Compression(val cls: String, val params: Map<String, String>) {

    // helpers
    fun getChunkLength(): String {
        for ((key, value) in params) {
            if (key == "chunk_length_in_kb" || key == "chunk_length_kb")
                return value
        }
        // default for every version we know of
        return "64"
    }

    fun getShortName(): String {
        return cls.replace("org.apache.cassandra.io.compress.", "")
    }
}