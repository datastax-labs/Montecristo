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

package com.datastax.dsestatsconverter

import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

fun main(args: Array<String>) {
    println("Convert called")
    if (args.size != 1) {
        println("Incorrect number of arguments provided to the conversion")
    }
    val baseFolder = args[0]
    val convert = Convert()
    convert.execute(baseFolder)
}

class Convert() {
    private val logger = LoggerFactory.getLogger(this::class.java)

    fun execute(artifactsDirectory: String) {
        logger.info("base folder : $artifactsDirectory")
        val startTime = Calendar.getInstance().time.toString()
        logger.info("starting processing : $startTime ")
        val fileList = getFilesWithName(artifactsDirectory, ".*-Statistics\\.db")
        val numberOfFiles = fileList.size
        logger.info("$numberOfFiles files found to process")
        SSTableStatisticsConverter.execute(fileList)
        val endTime = Calendar.getInstance().time.toString()
        logger.info("Completed table metadata processing : $endTime")
    }

    private fun getFilesWithName(artifactsDirectory: String, pattern: String): List<File> {
        val regEx = Regex(pattern)
        return File(artifactsDirectory)
            .walkTopDown()
            .asSequence()
            .filter { f -> f.name.matches(regEx) }
            .toList()
    }
}