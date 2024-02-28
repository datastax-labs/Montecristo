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

package com.datastax.montecristo.helpers

import java.io.File

object FileHelpers {

    // TODO : These all now need unit tests creating, they are not pure - since access file systems
    // IoC where possible has been used though to help with tests
    // e.g. the folder they look in is passed in and not pulled directly from artifact object

    fun getFile(rootDirectory: File, fileNames: List<String>): File {
        // because some files move paths, we allow a list to be submitted and we check in order
        // ntp for example went from network/ntpq to os/ntpq - so we need to be able to be tolerant of these changes.
        fileNames.forEach { fileName ->
            val file = File(rootDirectory, fileName)
            if (file.exists() && !file.isDirectory) {
                return file
            }
            val fileWithExtention = File(rootDirectory, "$fileName.txt")
            if (fileWithExtention.exists()) {
                return fileWithExtention
            }
        }
        return File("")
    }

    fun getFilesWithName(artifactsDirectory: String, pattern: String): List<File> {
        val regEx = Regex(pattern)
        return File(artifactsDirectory)
                .walkTopDown()
                .asSequence()
                .filter { f -> f.name.matches(regEx) }
                .toList()
    }

    fun getHostname(dir: File): String {
        val hostnameFile = getFile(dir, listOf("os/hostname", "os-metrics/hostname"))
        // try the folder name first
        val pattern = Regex("extracted/([^_]+)")
        val hostname = pattern.find(dir.absolutePath)?.groups?.get(1)?.value
        if (hostname.isNullOrBlank()) {
            // try the hostname file instead
            if (hostnameFile.exists()) {
                val hostnameFileLine = hostnameFile.readText().trim()
                if (hostnameFileLine != "localhost") {
                    return hostnameFileLine
                }
            }
            throw RuntimeException("Unable to determine hostname from either the folder or the hostname file")
        } else {
            return hostname.toString()
        }
    }
}