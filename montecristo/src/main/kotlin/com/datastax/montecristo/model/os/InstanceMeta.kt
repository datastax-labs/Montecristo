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

package com.datastax.montecristo.model.os

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper


data class Instance(val instanceType: String, val memory: String,
                    val vCPU: Int, val ebsOptimized: Boolean)

/***
 * Used to extract AWS information like cost, type vCPU count, ebsoptimized, contains a list of Instance types
 * @see Instance
 */
class InstanceMeta {
    private val factory: JsonFactory = JsonFactory()
    val json: JsonNode
    val instances = mutableMapOf<String, Instance>()

    init {
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS)
        val stream = this.javaClass.getResourceAsStream("/instances.json")
        val mapper = ObjectMapper()
        json = mapper.readTree(stream)

        json.forEach { x ->
            instances[x.get("instance_type").asText()] = Instance(instanceType = x.get("instance_type").asText(),
                memory = x.get("memory").asText(),
                vCPU = x.get("vCPU").asInt(),
                ebsOptimized = x.get("ebs_optimized").asBoolean())
        }
    }
    fun getInstance(name: String) : Instance? {
        return instances[name]
    }
}

/*
{
    "ECU": 1.0,
    "FPGA": 0,
    "GPU": 0,
    "arch": [
      "i386",
      "x86_64"
    ],
    "base_performance": null,
    "burst_minutes": null,
    "clock_speed_ghz": null,
    "ebs_iops": 0,
    "ebs_max_bandwidth": 0,
    "ebs_optimized": false,
    "ebs_throughput": 0,
    "enhanced_networking": false,
    "family": "General purpose",
    "generation": "previous",
    "instance_type": "m1.small",
    "intel_avx": null,
    "intel_avx2": null,
    "intel_turbo": null,
    "ipv6_support": false,
    "linux_virtualization_types": [
      "PV"
    ],
    "memory": 1.7,
    "network_performance": "Low",
    "physical_processor": null,
    "placement_group_support": false,
    "pretty_name": "M1 General Purpose Small",
    "pricing": {
      "ap-northeast-1": {
        "linux": {
          "ondemand": "0.061",
          "reserved": {
            "yrTerm1Standard.allUpfront": "0.032",
            "yrTerm1Standard.noUpfront": "0.037",
            "yrTerm1Standard.partialUpfront": "0.032",
            "yrTerm3Standard.allUpfront": "0.021",
            "yrTerm3Standard.partialUpfront": "0.022"
 */