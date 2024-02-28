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

import io.mockk.every
import io.mockk.mockk
import org.junit.Ignore
import org.junit.Test


internal class GenerateDiscoveryReportTest() {

    // This test is not part of the test suite, it is designed for local debugging where you wish to run the entire report
    // generation against a locally stored tarball and debug why a problem is occurring.
    // It doesn't spin up the webserver, its just doing the document generation and writing the output markdown files
    // The console shows the normal stdout logging but also breakpoints can be used
    // The @ignore is there to make sure it does not get included in a test suite for build purposes, the files / folder
    // it is debugging will not be in the repo. To run the actual test for debugging, comment the @Ignore out
    @Ignore
    @Test
    fun runReportGeneration() {
        // root directory
        val jira = "Some-Folder/Tarball-name-date"
        val homeFolder = System.getProperty("user.home")
        val rootDirectory = "$homeFolder/ds-discovery/$jira"
        val discoveryArgs = mockk<DiscoveryArgs>(relaxed = true)
        every { discoveryArgs.rootDirectory } returns rootDirectory

        val p = java.nio.file.Paths.get(rootDirectory).toAbsolutePath()
        GenerateDiscoveryReport(p.toString(), discoveryArgs).execute()
    }
}