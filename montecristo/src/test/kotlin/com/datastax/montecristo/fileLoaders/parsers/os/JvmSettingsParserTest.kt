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

package com.datastax.montecristo.fileLoaders.parsers.os

import com.datastax.montecristo.fileLoaders.parsers.application.JVMSettingsParser
import com.datastax.montecristo.model.os.GCAlgorithm
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class JvmSettingsParserTest {

    val commandPrefix= "cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java"
    val commandSuffix= "CassandraDaemon"

    val CMS_ArgAsList=listOf("-XX:+UseParNewGC","-XX:+UseConcMarkSweepGC","-XX:+CMSParallelRemarkEnabled")
    val CMS_Arg=CMS_ArgAsList.joinToString(separator=" ")
    val CMS_Mem="-Xms4G -Xmx4G -Xmn600M"

    val G1_ArgAsList=listOf("-XX:+UseG1GC","-XX:+ParallelRefProcEnabled","-XX:NewSize=12000m","-XX:G1HeapRegionSize=16m", "-XX:ParallelGCThreads=16", "-XX:ConcGCThreads=16")
    val G1_Arg=G1_ArgAsList.joinToString(separator=" ")
    val G1_Mem="-Xms31G -Xmx31G"

    val ZGC_ArgAsList=listOf("-XX:+UseZGC","-XX:+ParallelRefProcEnabled","-XX:NewSize=12000m","-XX:G1HeapRegionSize=16m", "-XX:ParallelGCThreads=16", "-XX:ConcGCThreads=16")
    val ZGC_Arg=ZGC_ArgAsList.joinToString(separator=" ")
    val ZGC_Mem="-Xms31G -Xmx31G"

    @Test
    fun cms() {
        val commandLine="${commandPrefix} ${CMS_Arg} ${CMS_Mem} ${commandSuffix}"
        val jvmSettings = JVMSettingsParser.parse(commandLine)

        assertThat(jvmSettings.gcAlgorithm).isEqualTo(GCAlgorithm.CMS)
        assertThat(jvmSettings.heapSize).isEqualTo(4294967296)
        assertThat(jvmSettings.newGenSize).isEqualTo(629145600)
        assertThat(jvmSettings.gcFlags.sorted()).isEqualTo(CMS_ArgAsList.sorted())
    }

    @Test
    fun g1() {
        val commandLine="${commandPrefix} ${G1_Arg} ${G1_Mem} ${commandSuffix}"
        val jvmSettings = JVMSettingsParser.parse(commandLine)

        assertThat(jvmSettings.gcAlgorithm).isEqualTo(GCAlgorithm.G1GC)
        assertThat(jvmSettings.heapSize).isEqualTo(33285996544)
        assertThat(jvmSettings.newGenSize).isEqualTo(12582912000)
        assertThat(jvmSettings.gcFlags.sorted()).isEqualTo(G1_ArgAsList.sorted())
    }

    @Test
    fun zgc() {
        val commandLine="${commandPrefix} ${ZGC_Arg} ${ZGC_Mem} ${commandSuffix}"
        val jvmSettings = JVMSettingsParser.parse(commandLine)

        assertThat(jvmSettings.gcAlgorithm).isEqualTo(GCAlgorithm.UNKNOWN)
    }

    @Test
    fun unspecified() {
        val commandLine="${commandPrefix} ${commandSuffix}"
        val jvmSettings = JVMSettingsParser.parse(commandLine)

        assertThat(jvmSettings.gcAlgorithm).isEqualTo(GCAlgorithm.UNKNOWN)
    }

    @Test
    fun g1_withXmn() {
        val commandLine="${commandPrefix} ${G1_Arg} ${G1_Mem} -Xmn600M ${commandSuffix}"
        val jvmSettings = JVMSettingsParser.parse(commandLine)

        assertThat(jvmSettings.gcAlgorithm).isEqualTo(GCAlgorithm.G1GC)
        assertThat(jvmSettings.heapSize).isEqualTo(33285996544)
        assertThat(jvmSettings.newGenSize).isEqualTo(12582912000)
        assertThat(jvmSettings.gcFlags.sorted()).isEqualTo(G1_ArgAsList.sorted())
    }

}