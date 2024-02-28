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

package com.datastax.montecristo.helpers

import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

internal class ByteCountHelperTest {
    @Test
    fun testHumanReadableByteCountSiUnits() {
        Locale.setDefault(Locale.ENGLISH);

        val units = ByteCountHelperUnits.SI

        assertEquals("324 B", ByteCountHelper.humanReadableByteCount(324, units))
        assertEquals("3.24 kB", ByteCountHelper.humanReadableByteCount(3_242, units))
        assertEquals("3.24 MB", ByteCountHelper.humanReadableByteCount(3_242_167, units))
        assertEquals("3.24 GB", ByteCountHelper.humanReadableByteCount(3_242_167_910, units))
        assertEquals("3.24 TB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456, units))
        assertEquals("3.24 PB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456_835, units))
        assertEquals("3.24 EB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456_835_170, units))
    }

    @Test
    fun testHumanReadableByteCountBinaryUnits() {
        Locale.setDefault(Locale.ENGLISH);

        val units = ByteCountHelperUnits.BINARY

        assertEquals("324 B", ByteCountHelper.humanReadableByteCount(324, units))
        assertEquals("3.17 KiB", ByteCountHelper.humanReadableByteCount(3_242, units))
        assertEquals("3.09 MiB", ByteCountHelper.humanReadableByteCount(3_242_167, units))
        assertEquals("3.02 GiB", ByteCountHelper.humanReadableByteCount(3_242_167_910, units))
        assertEquals("2.95 TiB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456, units))
        assertEquals("2.88 PiB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456_835, units))
        assertEquals("2.81 EiB", ByteCountHelper.humanReadableByteCount(3_242_167_910_456_835_170, units))
    }

    @Test
    fun testParseHumanReadableByteCountToLongUnrecognisedUnits() {
        assertEquals(-1, ByteCountHelper.parseHumanReadableByteCountToLong("1.0 QB"))
        assertEquals(-1, ByteCountHelper.parseHumanReadableByteCountToLong("1.0 kR"))
    }


    @Test
    fun testParseHumanReadableByteCountToLongBinaryUnits() {
        assertEquals(8_589_934_592, ByteCountHelper.parseHumanReadableByteCountToLong("8GiB"))
        assertEquals(8_589_934_592, ByteCountHelper.parseHumanReadableByteCountToLong("8 GiB"))
        assertEquals(32_657_530_880, ByteCountHelper.parseHumanReadableByteCountToLong("31892120 KiB"))
    }

    @Test
    fun testParseHumanReadableByteCountToLongSiUnits() {
        assertEquals(8_000_000_000, ByteCountHelper.parseHumanReadableByteCountToLong("8GB"))
        assertEquals(8_000_000_000, ByteCountHelper.parseHumanReadableByteCountToLong("8 GB"))
        assertEquals(32_000, ByteCountHelper.parseHumanReadableByteCountToLong("32 KB"))
        assertEquals(32_000_000, ByteCountHelper.parseHumanReadableByteCountToLong("32000kB"))
    }
}