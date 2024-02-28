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

package com.datastax.montecristo.commands

import com.beust.jcommander.Parameter
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Paths


enum class DocumentType(val file: String) {
    EXPORTER("exporter.md"),
    ASTRA("astra.md")
}

class GenerateSingleDocument {
    @Parameter(description = "Hugo top level directory")
    var dir: String = ""

    private val logger = LoggerFactory.getLogger(this.javaClass)

    fun generateSingleDocument() {
        val p = Paths.get(dir).toAbsolutePath().toString()
        val directory = File(p, "content")

        logger.info("Using $directory")

        for (documentType in DocumentType.values()) {
            // Gather up list of files  and make sure they are sorted, we know [0,2] is the sort string.
            // Skip over the 001_intro file because it is optionally included depending on the type of document being
            // generated.
            val sortedFiles = directory
                    .listFiles()
                    .filter {
                        it.extension == "md"
                            && it.nameWithoutExtension != "_index"
                            && it.nameWithoutExtension != "exporter"
                            && (documentType != DocumentType.ASTRA || it.nameWithoutExtension.contains("astra_"))
                    }
                    .sorted()

            logger.info("Opening ${documentType.file}")
            val final = File(directory, documentType.file)

            logger.info("Writing front matter to ${documentType.file}")

            val frontMatter = """+++
                |title = "DataStax Services Document"
                |show_nav = false
                |show_discovery_banner = false
                |+++
                |
                |
                """.trimMargin()

            final.writeText(frontMatter)

            // We need to use the builder here because the we need the complete doc to extract the headers. This is
            // in case a Table of Contents (ToC) need to be built. In this case the headers build the ToC.
            val content = StringBuilder()

            for (f in sortedFiles) {
                logger.info("Appending $f")
                content.appendLine(f.readText())
                content.appendLine()
            }

            final.appendText(content.toString())
        }
    }
}
