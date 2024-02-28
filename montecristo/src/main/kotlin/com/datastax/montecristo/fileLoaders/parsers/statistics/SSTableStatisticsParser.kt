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

package com.datastax.montecristo.fileLoaders.parsers.statistics

import com.datastax.montecristo.model.metrics.SSTableStatistics
import com.datastax.montecristo.model.metrics.SingleSSTableStatistic
import org.apache.cassandra.tools.SSTableMetadataViewer
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.nio.file.Paths

object SSTableStatisticsParser {

    fun parse(files : List<File>): SSTableStatistics {

        val results = files.map { file ->
            try {
                val rawData = if (file.name.endsWith(".db")) {
                    // first part, set up to capture system.out which is where the sstablemetaviewer returns result to.
                    val oldSystemOut = System.out
                    val outputStream = ByteArrayOutputStream()
                    val capture = PrintStream(outputStream)

                    // set up a cassandra.yaml as a configured system item, the metadata command wants this since it loads this information, even though
                    // it does not directly use it.
                    val yamlPath = File(Paths.get("").toAbsolutePath().toString() + "/sstablemetadata-cassandra.yaml").toURI()
                    System.setProperty("cassandra.config", yamlPath.toString())
                    // run the metaviewer command
                    System.setOut(capture)
                    SSTableMetadataViewer.main(arrayOf(file.absolutePath))

                    // ensure all text flushed and return to the previous system.out
                    System.out.flush()
                    System.setOut(oldSystemOut)
                    // start parsing
                    outputStream.toString().split("\n")
                } else {
                    // simple text file, run it
                    file.readLines()
                }

                // to get the keyspace / tablename, we need to parse the file path
                val ksTbl = extractKeyspaceTableFromFile(file)
                // to get the row count, parse the data itself
                val rowCountText = rawData.firstOrNull { it.startsWith("totalRows") } ?: "totalRows: 0"
                val repairedAtText = rawData.firstOrNull { it.startsWith("Repaired at")} ?: "Repaired at: 0"
                val rowCount = rowCountText.split(":")[1].trim().toLong()
                val repairedAt = repairedAtText.split(":")[1].trim().toLong()
                val hasBeenIncrementallyRepaired = repairedAt > 0
                SingleSSTableStatistic(ksTbl, rowCount, hasBeenIncrementallyRepaired)
            } catch (ex : Exception) {
                // unable to read the file
                val ksTbl = extractKeyspaceTableFromFile(file)
                SingleSSTableStatistic(ksTbl,0, false)
            }
        }.toList()
        return SSTableStatistics(results)
    }

    internal fun extractKeyspaceTableFromFile(file : File) : String {
        // The file exists in a path which is along the lines of
        //    /cassandra-data-folder/sub-folder-etc/keyspace/table-uuid/statistic-file-name.db
        val tableName = file.parentFile.name.split("-")[0]
        val keyspaceName = file.parentFile.parentFile.name
        return "$keyspaceName.$tableName"
    }
}
