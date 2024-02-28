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

package com.datastax.montecristo.fileLoaders.parsers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.profiles.ExecutionProfile
import java.io.File
import java.lang.Exception

object ExecutionConfigParser {

        fun parse(configFile: File, loadErrors : MutableList<LoadError>): ExecutionProfile {
            val mapper = ObjectMapper(YAMLFactory())
            mapper.registerModule(
                KotlinModule.Builder()
                    .withReflectionCacheSize(512)
                    .configure(KotlinFeature.NullToEmptyCollection, false)
                    .configure(KotlinFeature.NullToEmptyMap, false)
                    .configure(KotlinFeature.NullIsSameAsDefault, false)
                    .configure(KotlinFeature.SingletonSupport, false)
                    .configure(KotlinFeature.StrictNullChecks, false)
                    .build()
            )
            return try {
                mapper.readValue (configFile, ExecutionProfile::class.java)
            } catch (exception: Exception) {
                println("Could not read Execution Profile YAML file - using default instead")
                loadErrors.add(LoadError("All", "Unable to load the chosen execution profile, using the default profile."))
                ExecutionProfile.default()
            }
        }
    }