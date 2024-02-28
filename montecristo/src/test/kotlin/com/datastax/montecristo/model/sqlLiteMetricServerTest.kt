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

package com.datastax.montecristo.model

import com.datastax.montecristo.model.metrics.ServerMetricList
import org.junit.Test
import kotlin.test.assertEquals


class sqlLiteMetricServerTest {
    @Test
    fun testHistogramServerList() {

        var metrics = ServerMetricList(mutableListOf(Pair("node1", 10.0), Pair("node1", 20.0)))
        assertEquals(20.0, metrics.max())
        assertEquals(10.0, metrics.min())
        assertEquals(15.0, metrics.average())

    }
//    @Test
//    fun testCastToInt() {
//        var metrics : ServerMetricList<String> = mutableListOf(Pair("node1", "10"), Pair("node1", "20"))
//
//        var metrics2 : ServerMetricList<Int> = metrics.toInt()
//        assertEquals(10, metrics2.first().second)
//
//
//    }
}