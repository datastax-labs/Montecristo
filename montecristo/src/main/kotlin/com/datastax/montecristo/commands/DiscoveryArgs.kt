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
import com.beust.jcommander.Parameters

@Parameters(commandDescription="Generates a montecristo document.")
class DiscoveryArgs : BaseCommand {
    @Parameter(description = "Path to montecristo files")
    var rootDirectory : String = ""

    @Parameter(names = ["--version", "-v"], description = "Use a specific version rather than the discovered one.")
    var version : String = ""


    @Parameter(names = ["--executionProfile", "-e"], description = "The execution config file to use, default is execution.config.")
    var executionProfilePath : String = "execution.config"

    override fun execute() {
        val p = java.nio.file.Paths.get(rootDirectory).toAbsolutePath()
        GenerateDiscoveryReport(p.toString(), this).execute()
    }

    @Parameter(names = ["--dry-run"], description = "Don't actually write out the files or touch any directory")
    var dryRun = false
}