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

import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.test.assertTrue

internal class ConvertTest() {

    @Test
    fun testConvertWithSharedTestFiles() {
        // Use shared test Statistics.db files from montecristo module
        val projectRoot = Paths.get("").toAbsolutePath().parent
        val testFilesPath = projectRoot.resolve("montecristo/src/test/resources/fileLoaders/parsers/sstable-statistics")
        
        // Create output directory in build/test-output
        val outputDir = Paths.get("build/test-output/sstable-statistics").toFile()
        outputDir.mkdirs()
        
        // Find all Statistics.db files
        val statisticsFiles = File(testFilesPath.toString())
            .walkTopDown()
            .filter { it.name.endsWith("-Statistics.db") }
            .toList()
        
        assertTrue(statisticsFiles.isNotEmpty(), "Should find at least one Statistics.db file")
        
        // Copy Statistics.db files to output directory, preserving directory structure
        val copiedFiles = statisticsFiles.map { dbFile ->
            val relativePath = testFilesPath.relativize(dbFile.toPath())
            val targetFile = outputDir.toPath().resolve(relativePath).toFile()
            targetFile.parentFile.mkdirs()
            Files.copy(dbFile.toPath(), targetFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            targetFile
        }
        
        // Process the Statistics.db files in the output directory
        Convert().execute(outputDir.absolutePath)
        
        // Verify that .txt files were created for each Statistics.db file
        copiedFiles.forEach { dbFile ->
            val txtFile = File(dbFile.absolutePath.substringBeforeLast(".") + ".txt")
            assertTrue(
                txtFile.exists() && txtFile.length() > 0,
                "Expected output file ${txtFile.name} to exist and be non-empty. " +
                "If this fails, check that all required DSE jars are present in .dse-libs/ directory."
            )
        }
    }

}