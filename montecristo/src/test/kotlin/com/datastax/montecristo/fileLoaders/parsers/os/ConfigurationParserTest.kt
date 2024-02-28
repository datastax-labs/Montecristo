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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertEquals

internal class ConfigurationParserTest {

    @Test
    fun testMeminfoParse() {
        val memTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/meminfo").reader().readLines()
        val meminfo = MemInfoParser.parse (memTxt)
        assertEquals(16690092, meminfo.cached)
        assertEquals(32825216, meminfo.memTotal)
        assertEquals(6614680, meminfo.memFree)
        assertEquals(23958376, meminfo.memAvailable)
        assertEquals(16690092, meminfo.cached)
        assertEquals(33431036, meminfo.swapTotal)
        assertEquals(33405180, meminfo.swapFree)
    }

    @Test
    fun testSysCtlSwappiness() {
        val systxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/sysctl.txt").reader().readLines()
        val sys = SysCtlParser.parse(systxt)
        assertThat(sys.vmswappiness).isEqualTo("60")
        assertThat( sys.get("debug.exception-trace")).isEqualTo("1")
    }

    @Test
    fun testDebianParse() {
        val lscpuDebianTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/lscpu.txt").reader().readLines()
        val lscpu = LsCpuParser.parse(lscpuDebianTxt)
        assertThat(lscpu.getCpuThreads()).isEqualTo(12)
        assertThat(lscpu.sockets).isEqualTo(12)
        assertThat(lscpu.getModelName()).contains("CPU E5-2660")
        assertThat(lscpu.hypervisorVendor).isEqualToIgnoringCase("vmware")
    }


    /**
     * Tests that we have each of the correct info in the CPU usage
     */
    @Test
    fun testDStatFirstDataRow() {
        val dstatTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/dstat-example1.csv").reader().readLines()
        val dstat = DstatParser.parse(dstatTxt)
        val ts = dstat.data.first()
        // 4.003,
        // 0.991
        // 94.967,
        // 0.040,
        // 0
        assertThat(ts.cpu.usr).isEqualTo(4.003)
        assertThat(ts.cpu.sys).isEqualTo(0.991)
        assertThat(ts.cpu.idl).isEqualTo(94.967)
        assertThat(ts.cpu.wai).isEqualTo(0.040)
        assertThat(ts.cpu.stl).isEqualTo(0.0)

        assertThat(ts.disk.read).isEqualTo(197519.728)
        assertThat(ts.disk.write).isEqualTo(352259.941)

        assertThat(ts.network.recv).isEqualTo(0)
        assertThat(ts.network.send).isEqualTo(0)
    }

    @Test
    fun testDStatNumberFormatIssue() {
        val dstatTxt = this.javaClass.getResourceAsStream("/fileLoaders/parsers/os/dstat-example2.csv").reader().readLines()
        val dstat = DstatParser.parse(dstatTxt)
        val ts = dstat.data.first()
        assertThat(ts.cpu.usr).isEqualTo(7.361)
        assertThat(ts.cpu.sys).isEqualTo(1.109)
        assertThat(ts.cpu.idl).isEqualTo(90.243)
        assertThat(ts.cpu.wai).isEqualTo(1.287)
        assertThat(ts.cpu.stl).isEqualTo(0.0)

        assertThat(ts.disk.read).isEqualTo(3684516.924)
        assertThat(ts.disk.write).isEqualTo(1300968.301)

        assertThat(ts.network.recv).isEqualTo(0)
        assertThat(ts.network.send).isEqualTo(0)

        assertThat(ts.paging.`in`).isEqualTo(2099) // value decimals are truncated, so floors.
        assertThat(ts.paging.out).isEqualTo(2562) // value decimals are truncated, so floors.

    }
}