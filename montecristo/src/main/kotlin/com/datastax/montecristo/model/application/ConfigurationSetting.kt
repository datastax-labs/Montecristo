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

package com.datastax.montecristo.model.application


data class ConfigurationSetting(val name: String, val values: Map<String, ConfigValue>) {

    // helpers
    fun isConsistent() = getDistinctValues().size == 1

    fun areAllUnset() = values.filter { it.value.isSet }.isEmpty()

    fun getDistinctValues(): Set<ConfigValue> {
        return values.map { it.value }.toSet()
    }

    // this helper is for when you are in a isConsistent=true scenario, it stops the boiler plate .entries.first.value.value occurring throughout the code
    fun getSingleValue(): String {
        return values.entries.first().value.getConfigValue()
    }
}

