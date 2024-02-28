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

package com.datastax.montecristo.fileLoaders.parsers.nodetool

import com.datastax.montecristo.model.nodetool.Ring
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.math.BigInteger
import kotlin.test.assertEquals

class RingParserTest {


    lateinit var singleDcRing : List<Ring>
    lateinit var singleDcSingleRing : List<Ring>
    lateinit var multiDcRing : List<Ring>
    lateinit var weirdRackNamingRing : List<Ring>
    lateinit var spaceRing : List<Ring>
    lateinit var randomRing : List<Ring>

    @Before
    fun setUp() {
        val spaceRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/ringWithSpacing.txt").reader().readLines()
        spaceRing = RingParser.parse(spaceRingFile)
        val singleDcRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/single_dc_ring.txt").reader().readLines()
        singleDcRing = RingParser.parse(singleDcRingFile)
        val singleDcSingleNodeFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/single_dc_singlenode_ring.txt").reader().readLines()
        singleDcSingleRing = RingParser.parse(singleDcSingleNodeFile)
        val multiDcRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/multi_dc_ring.txt").reader().readLines()
        multiDcRing = RingParser.parse(multiDcRingFile)
        val weirdRackNamingRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/weird_rack_naming_ring.txt").reader().readLines()
        weirdRackNamingRing = RingParser.parse(weirdRackNamingRingFile)
        val randomRingFile = this.javaClass.getResourceAsStream("/fileLoaders/parsers/nodetool/ring/randomPartitonerRing.txt").reader().readLines()
        randomRing = RingParser.parse(randomRingFile)
    }

    @Test
    fun testRingWithSpacingParses() {
        assertThat(spaceRing.size).isEqualTo(2)
        assertThat(spaceRing.get(0).datacenter).isEqualTo("abcdefgh")
        assertThat(spaceRing.get(1).datacenter).isEqualTo("ijklmnopq")
        assertThat(spaceRing.get(0).nodes.count()).isEqualTo(3)

    }

    @Test
    fun testSingleDCNodeCount() {
        assertThat(singleDcRing.size).isEqualTo(1)
        assertThat(singleDcRing.get(0).datacenter).isEqualTo("single_dc")
        assertThat(singleDcRing.get(0).nodes.count()).isEqualTo(4)

    }

    @Test
    fun testSingleDCSingleNodeCount() {
        assertThat(singleDcSingleRing.size).isEqualTo(1)
        assertThat(singleDcSingleRing.get(0).datacenter).isEqualTo("single_node_dc")
        assertThat(singleDcSingleRing.get(0).nodes.count()).isEqualTo(1)
    }

    @Test
    fun testMultiDCNodeCount() {
        assertThat(multiDcRing.get(0).nodes.count()).isEqualTo(5)
        assertThat(multiDcRing.get(1).nodes.count()).isEqualTo(5)
        assertThat(multiDcRing.get(2).nodes.count()).isEqualTo(6)
        assertThat(multiDcRing.get(3).nodes.count()).isEqualTo(3)
    }

    @Test
    fun testMultiDCsIdentified() {
        assertEquals("first_dc", multiDcRing.get(0).datacenter)
        assertEquals("second_dc", multiDcRing.get(1).datacenter)
        assertEquals("third_dc", multiDcRing.get(2).datacenter)
    }

    @Test
    fun testMultiDCStartEndForFirstEntryFirstDC() {
        assertEquals("first_dc", multiDcRing.get(0).datacenter)
        assertThat(multiDcRing.size > 0)
        assertThat(multiDcRing.get(0).nodes.contains("10.115.99.105"))
        assertThat(multiDcRing.get(0).nodes.get("10.115.99.105")!!.size > 0)
        assertEquals(BigInteger.valueOf(9191203510050875996), multiDcRing.get(0).nodes.get("10.115.99.105")!!.first().startRange)
        assertEquals(BigInteger.valueOf(-9201113874763842887), multiDcRing.get(0).nodes.get("10.115.99.105")!!.first().endRange)
    }

    @Test
    fun testMultiDCStartEndForLastEntryFirstDC() {
        assertEquals("first_dc", multiDcRing.get(0).datacenter)
        assertThat(multiDcRing.size > 0)
        assertThat(multiDcRing.get(0).nodes.contains("10.115.99.103"))
        assertThat(multiDcRing.get(0).nodes.get("10.115.99.103")!!.size > 0)
        assertEquals(BigInteger.valueOf(9055475057956395545), multiDcRing.get(0).nodes.get("10.115.99.103")!!.last().startRange)
        assertEquals(BigInteger.valueOf(9191203510050875995), multiDcRing.get(0).nodes.get("10.115.99.103")!!.last().endRange)
    }

    @Test
    fun testMultiDCStartEndForFirstEntryLastDC() {
        assertEquals("last_dc", multiDcRing.get(3).datacenter)
        assertThat(multiDcRing.size > 0)
        assertThat(multiDcRing.get(3).nodes.contains("10.115.66.109"))
        assertThat(multiDcRing.get(3).nodes.get("10.115.66.109")!!.size > 0)
        assertEquals(BigInteger.valueOf(8771493070214529337), multiDcRing.get(3).nodes.get("10.115.66.109")!!.first().startRange)
        assertEquals(BigInteger.valueOf(-8690440845636107390), multiDcRing.get(3).nodes.get("10.115.66.109")!!.first().endRange)
    }

    @Test
    fun testMultiDCStartEndForLastEntryLastDC() {
        assertEquals("last_dc", multiDcRing.get(3).datacenter)
        assertThat(multiDcRing.size > 0)
        assertThat(multiDcRing.get(3).nodes.contains("10.115.66.109"))
        assertThat(multiDcRing.get(3).nodes.get("10.115.66.109")!!.size > 0)
        assertEquals(BigInteger.valueOf(7998138290864234575), multiDcRing.get(3).nodes.get("10.115.66.109")!!.last().startRange)
        assertEquals(BigInteger.valueOf(8771493070214529336), multiDcRing.get(3).nodes.get("10.115.66.109")!!.last().endRange)
    }

    @Test
    fun testWeirdRacksNodeCount() {
        assertThat(weirdRackNamingRing.size).isEqualTo(1)
        assertThat(weirdRackNamingRing.get(0).datacenter).isEqualTo("ld6")
        assertThat(weirdRackNamingRing.get(0).nodes.count()).isEqualTo(7)
    }

    @Test
    fun testRandomRingNodeCount() {
        assertThat(randomRing.size).isEqualTo(2)
        assertThat(randomRing.get(0).datacenter).isEqualTo("DC1")
        assertThat(randomRing.get(0).nodes.count()).isEqualTo(6)

    }
}