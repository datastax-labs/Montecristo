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

package com.datastax.montecristo.metrics

import com.datastax.montecristo.helpers.FileHelpers
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.util.regex.Pattern

class Server(val sqlLiteMetricServer: SqlLiteMetricServer, private val dir: File, val hostname: String, private val hasJmx : Boolean) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun load() {

      var az: String
      var instanceType: String

      val mem: Int = try {
        val meminfo = FileHelpers.getFile(dir, listOf("os/meminfo", "os-metrics/meminfo"))
        val first = BufferedReader(FileReader(meminfo)).readLine()

        val regex = Pattern.compile("MemTotal:\\s*(\\d*)\\s*kB").matcher(first)
        regex.find()

        (regex.group(1).toInt() / 1024 / 1024)
      } catch (e: FileNotFoundException) {
        logger.error("No meminfo in ${dir.absolutePath}")
        -1
      }

        try {
            az = File(dir, "cloud/aws-az.txt").readText()
            instanceType = File(dir, "cloud/aws-instance-type.txt").readText()
        } catch (e: Exception) {
            println(e.message)
            az = ""
            instanceType = ""
        }

        try {
            sqlLiteMetricServer.insertServer(hostname, mem, az, instanceType, hasJmx)
        } catch (e: org.sqlite.SQLiteException) {
            val resultCode = e.resultCode
            if (
                    resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY
                    ||
                    resultCode == org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT
            ) {
                logger.warn(e.message)
                logger.warn("Duplicate entry found for servers.host '$hostname'. $resultCode. Ignoring.")
            } else {
                throw e
            }
        }
    }
}