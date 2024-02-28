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

data class CPUUsage(val usr: Double, val sys: Double, val idl: Double, val wai: Double, val stl: Double) {
    companion object {
        /**
         * Needed for testing
         */
        fun empty(): CPUUsage {
            return CPUUsage(0.0, 0.0, 0.0, 0.0, 0.0)
        }

        fun fromCSVRecord(csv: CSVRecord): CPUUsage {
            return CPUUsage(
                csv.get(0).toDouble(),
                csv.get(1).toDouble(),
                csv.get(2).toDouble(),
                csv.get(3).toDouble(),
                csv.get(4).toDouble()
            )
        }
    }
}
