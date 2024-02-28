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

import com.datastax.montecristo.metrics.jmxMetrics.Metric
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import kotlin.test.fail

class MetricsReaderTest {

    @Test
    fun testMetricParsing() {
        var f = this::class.java.getResource("/metrics.jmx")


        var reader = BufferedReader(FileReader(File(f!!.file)))
        var tmp = MetricsReader("test", reader)

        var parser = MetricParser()
        var worked = 0
        for(row in tmp.iter()) {
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
//        fp.deleteOnExit()
        var db = SqlLiteMetricServer(fp.path, true)

        var f = this::class.java.getResource("/metrics.jmx")
        var reader = BufferedReader(FileReader(File(f!!.file)))

        var metrics = MetricsReader("test", reader)
        metrics.parseAndLoad(db, "plain")

    }

    @Test
    fun testMetricsReader20() {
        var f = this::class.java.getResource("/metrics2.0.jmx")
        var reader = BufferedReader(FileReader(File(f!!.file)))
        var metrics = MetricsReader("test", reader)

        var metricCount = 0


        for(m in metrics.iter()) {
            var parser = MetricParser()
            val parsed = parser.parseMetric(m, "node1")
            if(parsed is Metric && parsed.mtype == "DroppedMessage") {
                metricCount++
                println(parsed.toString())
            }

        }

        assertThat(metricCount).isGreaterThan(50)
    }

    @Test
    fun testLoadingMetricsFromCFStats() {
        val fp = File.createTempFile("/tmp", ".db")
        val db = SqlLiteMetricServer(fp.path, true)

        val f = File(this.javaClass.getResource("/fileLoaders/parsers/metrics/cfstats.txt").path)
        val metrics = CFStatsMetricLoader(db, f, "node1")

        metrics.load()
        val writeCount = db.getHistogram("abcd", "monitoring_payload", "WriteLatency", "Count" )
        assertThat(writeCount.max()).isEqualTo(1815904.0)
        val writeLatency = db.getHistogram("abcd", "monitoring_payload", "WriteLatency", "Mean" )
        assertThat(writeLatency.max()).isEqualTo(29.0) // 0.029ms, but in microseconds, it becomes 0.029 * 1000 - to match JMX units
    }

}