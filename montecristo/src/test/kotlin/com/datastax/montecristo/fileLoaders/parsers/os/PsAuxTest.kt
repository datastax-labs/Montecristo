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

package com.datastax.montecristo.fileLoaders.parsers.os;

import com.datastax.montecristo.model.os.PsAux
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

public class PsAuxTest {

    @Test
    fun user_cassandra_partial() {
        val testPsAux = PsAux(listOf("cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 CassandraDaemon"), "/some/path")
        assertThat(testPsAux.getCassandraRunningUser()).isEqualTo("cassandra")
    }

    @Test
    fun user_cassandra_full() {
        val testPsAux = PsAux(listOf("cassandra 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 CassandraDaemon"), "/some/path")
        assertThat(testPsAux.getCassandraRunningUser()).isEqualTo("cassandra")
    }

    @Test
    fun user_otheruser() {
        val testPsAux = PsAux(listOf("otheruser 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 CassandraDaemon"), "/some/path")
        assertThat(testPsAux.getCassandraRunningUser()).isEqualTo("otheruser")
    }

    @Test
    fun complete_line_cassandra_class() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "CassandraDaemon"
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun complete_line_dse_class() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "com.datastax.bdp.DseModule"
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun partial_line_cassandra_share() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "-cp /usr/share/cassandra/lib/* "
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun partial_line_cassandra_local() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "-cp /usr/local/cassandra/lib/* "
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun partial_line_dse_share() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "-cp /usr/share/dse/cassandra/lib/* "
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun partial_line_dse_opt() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "-cp /opt/dse/cassandra/lib/* "
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine)
    }

    @Test
    fun partial_line_unknown() {
        val commandLine="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "-cp /var/opt/weirdcassandra/lib/* "
        val testPsAux = PsAux(listOf(commandLine), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isNull()
    }

    @Test
    fun multipleLines() {
        val commandLine1="cassand+ 14224  2.4 68.4 24969288 22431316 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "CassandraDaemon"
        val commandLine2="cassand+ 14224  2.4 68.4 24969289 22431317 ?   SLl   2020 31132:11 "+
                "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-1.el7_6.x86_64/jre//bin/java "+
                "CassandraDaemon"
        val testPsAux = PsAux(listOf(commandLine1,commandLine2), "/some/path")

        assertThat(testPsAux.getNodeProcessLine()).isEqualTo(commandLine1)
    }

}
