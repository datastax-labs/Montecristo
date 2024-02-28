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
import com.datastax.montecristo.model.os.DStat.*
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser

object DstatParser : IFileParser<DStat> {

    override fun parse(data: List<String>): DStat {

        // reform the data so that we can push it through the csv parser
        val csvFile = CSVParser.parse(data.joinToString("\n"), CSVFormat.DEFAULT).iterator()
        val data = mutableListOf<TimeSlice>()

        /**
         * Raw headers, there's no reason to ever access these directly
         */
        val dstatAccouncement = csvFile.next() // pretty useless unless we want to parse multiple versions (we don't)
        val author = csvFile.next() // we do not need the author information
        val host = csvFile.next() // host machine name + user the command was run under, might be useful in future
        val cmd = csvFile.next() // cmdline executed

        /*"total cpu usage",,,,,        "dsk/total",,  "net/total",,"paging",,"system",,"memory usage",,,
        "usr","sys","idl","wai","stl","read","writ","recv","send","in","out","int","csw","used","free","buff","cach"*/
        val headersL1 = csvFile.next()
        val headersL2 = csvFile.next()
        csvFile.forEachRemaining { csv ->
            data.add(TimeSlice(
                    CPUUsage.fromCSVRecord(csv),
                    DiskActivity.fromCSVRecord(csv),
                    Network.fromCSVRecord(csv),
                    Paging.fromCSVRecord(csv),
                    System.fromCSV(csv),
                    Memory.fromCSV(csv)
            ))
        }
        return DStat(data)
    }
}