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

import org.junit.Test
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import kotlin.test.fail


class MetricsJsonReaderTest {

    @Test
    fun testMetricParsing() {
        var f = this::class.java.getResource("/metrics_jmx.json")


        var reader = BufferedReader(FileReader(File(f!!.file)))
        var tmp = MetricsReader("test", reader)

        var parser = MetricParser()
        var worked = 0
        for (row in tmp.iterJson()) {
            try {
                parser.parseMetric(row, "node1")
            } catch (e: IllegalStateException) {
                fail("Could not parse $row, $worked ok")
            }
            worked++

        }

    }

    @Test
    fun testFullMetricsReader() {
        // this isn't the most elegant test ever
        // it just runs through the metrics.jmx test
        // we should really do this for 2.0, 2.1, 2.2, 3.0, 3.x, 4.0
        // to be sure all of them work.
        // nice little TODO item i think...
        var fp = File.createTempFile("/tmp", ".db")
        var db = SqlLiteMetricServer(fp.path, true)

        var f = this::class.java.getResource("/metrics_jmx.json")
        var reader = BufferedReader(FileReader(File(f!!.file)))

        var metrics = MetricsReader("test", reader)
        metrics.parseAndLoad(db, "json")

    }
}