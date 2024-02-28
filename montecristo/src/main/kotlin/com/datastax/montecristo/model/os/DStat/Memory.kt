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

package com.datastax.montecristo.model.os.DStat

import org.apache.commons.csv.CSVRecord

data class Memory(val used: Long, val free: Long, val buff: Long, val cach: Long) {
    companion object {
        fun fromCSV(csv: CSVRecord): Memory {
            return Memory(csv.get(13).toDouble().toLong(),
                csv.get(14).toDouble().toLong(),
                csv.get(15).toDouble().toLong(),
                csv.get(16).toDouble().toLong())
        }
    }
}

