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

import com.datastax.montecristo.model.LoadError
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.File

class ExecutionConfigParserTest {

    @Test
    fun testExecutionProfileParse() {
        val yamlFile = File(this.javaClass.getResource("/fileLoaders/parsers/executionProfile/executionProfile.yaml").path)
        val loadErrors = mutableListOf<LoadError>()
        val executionProfile = ExecutionConfigParser.parse(yamlFile, loadErrors)
        assertThat(loadErrors.isEmpty()).isTrue
        assertThat(executionProfile.limits.numberOfLogDays).isEqualTo (90)
    }

    @Test
    fun testExecutionProfileParseMissing() {
        val yamlFile = File("/path/does/not/exist/executionProfile.yaml")
        val loadErrors = mutableListOf<LoadError>()
        val executionProfile = ExecutionConfigParser.parse(yamlFile, loadErrors)
        assertThat(loadErrors.isEmpty()).isFalse()
        assertThat(executionProfile.limits.numberOfLogDays).isEqualTo (90)
    }
}