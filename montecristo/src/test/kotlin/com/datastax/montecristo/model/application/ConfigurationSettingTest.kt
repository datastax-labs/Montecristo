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

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ConfigurationSettingTest() {

    @Test
    fun testDuplicateRemovalWhenDifferent() {
        var values: Map<String, ConfigValue> = mapOf(Pair("node1",ConfigValue(true, "","value_1")), Pair("node2",ConfigValue(true, "","value_2")))
        val cs = ConfigurationSetting("test", values)
        assertFalse(cs.isConsistent())
    }

    @Test
    fun testDuplicateRemovalWhenSame() {
        var values: Map<String, ConfigValue> = mapOf(Pair("node1",ConfigValue(true, "","same_value")), Pair("node2",ConfigValue(true, "","same_value")))
        val cs = ConfigurationSetting("test", values)
        assertTrue(cs.isConsistent())
    }

}