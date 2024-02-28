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

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.File

class ServerTest {

    @Test
    fun testInsetSameHostnameMultipleTimes() {
        /**
         * Test to make sure that we can handle the case where we try to insert data for host that already exists in
         * the metrics database.
         */
        var fp = File.createTempFile("/tmp", ".db")
        var db = SqlLiteMetricServer(fp.path, true)

        // This is a hack to get the path to the resource directory.
        var test_fh = File(this::class.java.getResource("/metrics/tm1.db").getPath()).getParentFile()

        var server_obj = Server(db, test_fh, "foobar", false)

        server_obj.load()

        // Call load() a second time so that it tries to insert into the same host key.
        var exceptionThrown = false
        try {
            server_obj.load()
        } catch (e: Exception) {
            println(e.message)
            exceptionThrown = true
        }

        assertThat(exceptionThrown).isEqualTo(false)
    }
}
