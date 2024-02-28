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

import com.datastax.montecristo.metrics.jmxMetrics.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.regex.Pattern


// precompiled bunch of regex
class MetricParser {
    // main method to call from external locations
    // will return an object matching the JMXMetric interface

    var pattern: Pattern =
        Pattern.compile("(?<metric>.*)\\{(?<labels>.*)\\}\\[(?<arrayindex>.*)\\](?<key>.*?):\\s*(?<value>.*)")!!
    var logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun parseMetric(str: String, node: String): JMXMetric {


        val parsed = getParsedExpression(str)
        val metricName = parsed.metric

        val keyspace = parsed.properties.getOrDefault("keyspace", "")
        var table = parsed.properties.getOrDefault("table", "")
        val scope = parsed.properties.getOrDefault("scope", "")

        val cf = parsed.properties.getOrDefault("columnfamily", "")
        val key = parsed.key
        val value = parsed.value.replace("NaN", "0.0")
        val mtype = parsed.properties.getOrDefault("type", "")
        val properties = parsed.properties
        val arrayIndices = parsed.arrayIndex.joinToString(".")

        val name = properties.getOrDefault("name", "")

        if (table == "" && cf != "") {
            table = cf
        }

        if(metricName == "org.apache.cassandra.metrics" ) {
            if(mtype == "ColumnFamily" || mtype == "Table") {
                if(table == "" && scope != "")
                    table = scope
                return TableHistogram(node, keyspace, table, name, key, value)
            }
            if(mtype == "Keyspace") {
                return KeyspaceHistogram(node, keyspace, properties, key, value)
            }
            return Metric(node, mtype, scope, name, key, value)
        }
        if(metricName == "org.apache.cassandra.db") {
            // there are cases where this metrics has no keyspace nor table:
            // org.apache.cassandra.db{type=Commitlog}[]CompletedTasks: 629048423
            // org.apache.cassandra.db{type=CompactionManager}[]CompletedTasks: 40242361
            // using just the key will clash, so we make it unique using the type attribute
            val uniqueKey = if (keyspace == "") "${properties.getOrDefault("type", "")}.$key" else key
            return DB(node, keyspace, table, uniqueKey, value)
        }
        if(metricName == "org.apache.cassandra.internal") {
            return Internal(node, mtype, key, value)
        }
        if(metricName == "org.apache.cassandra.request") {
            return Request()
        }
        if(metricName == "java.lang" && mtype == "MemoryPool") {
            val uniqueKey = "$arrayIndices.$key"
            return MemoryPool(node, name,  uniqueKey, value)
        }
        if(metricName == "org.apache.cassandra.service") {
            return Service(node, mtype, key, value)
        }
        if(metricName == "java.nio") {
            return Nio(node, mtype, name, key, value)
        }
        if(metricName == "java.lang" && mtype == "GarbageCollector") {
            val uniqueKey = "$arrayIndices.$key"
            return GarbageCollector(node, mtype, name, properties.getOrDefault("key", ""), uniqueKey, value)
        }
        if(metricName == "java.lang") {
            // so we can have these kinds of java.lang metrics
            // (1) java.lang{type=ClassLoading}[]LoadedClassCount: 6892.
            // (2) java.lang{type=Runtime, key=java.class.version}[]SystemProperties: 52.0.
            // (3) java.lang{type=MemoryManager, name=CodeCacheManager}[]Valid: true
            //
            // The (2) has explicit key inside labels, otherwise we consider key being the last thing before value
            // The (3) has a name property, which we consider a key too, but it's not unique enough so we append the OG key
            // If there is none of the above, we use the latest piece (LoadedClassCount in (1)) a key
            //
            // Trouble was that SystemProperties key is not unique enough and causes SQLITE_CONSTRAINT duplicity issues
            // as a reconciliation, we solve this by trying to get key from labels, otherwise use the later one
            //
            // and ofc, some java.lang metrics have array indices, which we need to use, if present
            val pkey = when {
              properties.containsKey("key") -> properties.getOrDefault("key", "")
              properties.containsKey("name") -> "${properties.getOrDefault("name", "")}.$key"
              else -> key
            }
            val uniqueKey = if (arrayIndices.isNotEmpty()) "$arrayIndices.$pkey" else pkey
            return Java(node, mtype, uniqueKey, value)
        }

        if (metricName == "org.apache.cassandra.net") {
            return Net()
        }

        if (metricName == "org.apache.cassandra.auth") {
            return Auth()
        }

        if (metricName == "org.apache.cassandra.transport") {
            return Transport()
        }
        return NoMatch()
    }

    fun getParsedExpression(str: String): ParsedExpression {
        val parsed = pattern.matcher(str)
        parsed.find()
        var metric = ""
        try {
            metric = parsed.group("metric")
        } catch (e: IllegalStateException) {
            // it's ok, we'll use a blank
        }

        val labels = parsed.group("labels").split(",")
        val propertiesList = labels.map { x -> x.split("=") }
        val properties = propertiesList.associateBy({ it[0].trim() }, { it[1].trim() })
        val arrayIndex = parsed.group("arrayindex").replace(" ", "").split(",")
        val key = parsed.group("key")
        val value = parsed.group("value")

        return ParsedExpression(metric, properties, arrayIndex, key, value)
    }
}