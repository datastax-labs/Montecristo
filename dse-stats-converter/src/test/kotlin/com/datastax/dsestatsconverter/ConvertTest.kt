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

import org.junit.Ignore
import org.junit.Test

internal class ConvertTest() {

    // This test is not part of the test suite, it is designed for local debugging.
    // The @ignore is there to make sure it does not get included in a test suite for build purposes, the files / folder
    // it is debugging will not be in the repo. To run the actual test for debugging, comment the @Ignore out
    @Test
    @Ignore
    fun runConvert() {
        // root directory
        val jira = "some-test"
        val homeFolder = System.getProperty("user.home")
        val rootDirectory = "$homeFolder/ds-discovery/$jira"

        val p = java.nio.file.Paths.get(rootDirectory).toAbsolutePath()
        Convert().execute(p.toString())
    }

}