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

package com.datastax.montecristo.model.metrics

import com.datastax.montecristo.metrics.IMetricServer

/**
 * Accepts the internal name of the metric we're going to be looking up, for example: ReadLatency
 * I realize this is a bit complex, but it allows us really trivial access to every histogram
 * If we don't do it this way we have a lot of manually written, inconsistent signatures
 *
 */

// TODO : these are metrics and should be moved out - having a direct use of the Dao here has forced it to be injected
// this should be pulled outwards, its not a schema object, but a metrics object
data class Histogram(val metricsServer: IMetricServer, val keyspace : String , val tableName : String,   val metricName: String) {

    val min = getHistogramData("Max")
    val p99 = getHistogramData("99thPercentile")
    var p95 = getHistogramData("95thPercentile")
    val max = getHistogramData("Max")
    val mean = getHistogramData("Mean")
    var count = getHistogramData("Count")

    // key is something like 99thPercentile
    private fun getHistogramData(key: String) : ServerMetricList {
        return metricsServer.getHistogram(keyspace, tableName, metricName, key)
    }
}

data class MetricValue(val metricsServer: IMetricServer,  val keyspace : String , val tableName : String,  val name: String) {
    val value = metricsServer.getHistogram(keyspace, tableName, name, "Value")
}

data class MetricCounter(val metricsServer: IMetricServer,  val keyspace : String , val tableName : String, val name: String) {
    val count =  metricsServer.getHistogram(keyspace, tableName, name, "Count")
}
