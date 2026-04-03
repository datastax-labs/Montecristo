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

import org.junit.Ignore
import org.junit.Test

internal class ConvertTest() {

    // This test uses a Statistics.db file from test resources
    @Test
    fun runConvertWithTestResource() {
        // Get the test resource file
        val resourceUrl = javaClass.classLoader.getResource("la-1-big-Statistics.db")
        require(resourceUrl != null) { "Test resource file not found" }
        
        val testFile = java.io.File(resourceUrl.toURI())
        require(testFile.exists()) { "Test file does not exist: ${testFile.absolutePath}" }
        
        // Run the converter on the file (converter will find it in the directory)
        Convert().execute(testFile.absolutePath)
    }

    // This test is not part of the test suite, it is designed for local debugging.
    // The @Ignore is there to make sure it does not get included in a test suite for build purposes, the files / folder
    // it is debugging will not be in the repo. To run the actual test for debugging, comment the @Ignore out
    @Test
    @Ignore
    fun runConvertWithCustomPath() {
        // root directory
        val jira = "some-test"
        val homeFolder = System.getProperty("user.home")
        val rootDirectory = "$homeFolder/ds-discovery/$jira"

        val p = java.nio.file.Paths.get(rootDirectory).toAbsolutePath()
        Convert().execute(p.toString())
    }

}