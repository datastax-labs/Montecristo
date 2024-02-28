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

import com.fasterxml.jackson.databind.JsonNode

data class DseYaml(val data : JsonNode)  : YamlConfig(data) {

    val authenticationEnabled get() = getValueFromPath("authentication_options.enabled", "false")
    val authorizationEnabled get() = getValueFromPath("authorization_options.enabled", "false")

}

