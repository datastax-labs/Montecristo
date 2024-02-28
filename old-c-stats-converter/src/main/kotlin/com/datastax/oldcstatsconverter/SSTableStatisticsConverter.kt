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

package com.datastax.oldcstatsconverter

import org.apache.cassandra.tools.SSTableMetadataViewer
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.jvm.isAccessible

object SSTableStatisticsConverter {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun execute(files: List<File>) {
        val numberOfFiles = files.size
        println("number of files : $numberOfFiles")
        // set up the cassandra config, it is totally fake but must exist
        val yamlPath = File(Paths.get("").toAbsolutePath().toString() + "/sstablemetadata-cassandra.yaml").toURI()
        System.setProperty("cassandra.config", yamlPath.toString())

        // set up the metadataviewer class which will be called repeatedly
        val oldSystemOut = System.out
        val outputStream = ByteArrayOutputStream()
        val capture = PrintStream(outputStream)

        files.map { file ->
            try {
                if (file.name.endsWith(".db")) {
                    System.setOut(capture)
                    SSTableMetadataViewer.main(arrayOf(file.absolutePath))
                    System.out.flush()
                    System.setOut(oldSystemOut)
                    // write output
                    val outputFileName = file.absolutePath.substringBeforeLast(".") + ".txt"
                    File(outputFileName).writeText(outputStream.toString())
                    outputStream.reset()
                }
            } catch (ex: Exception) {
                // unable to read the file
                logger.error(ex.stackTraceToString())
                logger.info("unable to read file $file")
            }
        }
    }

    inline fun <reified T> T.callPrivateFunc(name: String, vararg args: Any?): Any? =
        T::class
            .declaredMemberFunctions
            .firstOrNull { it.name == name }
            ?.apply { isAccessible = true }
            ?.call(this, *args)
}
