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

package com.datastax.montecristo

import com.beust.jcommander.JCommander
import com.datastax.montecristo.commands.BaseCommand
import com.datastax.montecristo.commands.DiscoveryArgs
import com.datastax.montecristo.commands.GenerateDB
import java.util.*

class CommandMain : BaseCommand {

    override fun execute() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

fun main(args: Array<String>) {
    /**
     * have to configure logback here
     * Look for ~/.montecristo/logback.xml first
     */
    Locale.setDefault(Locale.ENGLISH)
    val cm = CommandMain()
    val discovery = DiscoveryArgs()
    val generateDB = GenerateDB()

    val jc = JCommander.newBuilder()
            .addObject(cm)
            .addCommand("discovery", discovery)
            .addCommand("metrics", generateDB)
            .build()

    jc.programName = "Montecristo by DataStax"

    jc.parse(*args)

    when(jc.parsedCommand) {
        "discovery" -> discovery.execute()
        "metrics" -> generateDB.execute()
        else -> jc.usage()
    }
}




