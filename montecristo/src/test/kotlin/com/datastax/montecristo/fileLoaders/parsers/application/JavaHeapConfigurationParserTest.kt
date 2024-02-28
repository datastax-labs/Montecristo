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

package com.datastax.montecristo.fileLoaders.parsers.application

import com.datastax.montecristo.helpers.ByteCountHelper
import com.datastax.montecristo.helpers.ByteCountHelperUnits
import com.datastax.montecristo.model.os.GCAlgorithm
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class JavaHeapConfigurationParserTest {

    @Test
    fun testFullCommandLineParse() {
        val CASSANDRA_COMMAND = "cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48 /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java -Xloggc:/var/log/cassandra/gc.log -ea -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+HeapDumpOnOutOfMemoryError -Xss256k -XX:StringTableSize=1000003 -XX:+AlwaysPreTouch -XX:-UseBiasedLocking -XX:+UseTLAB -XX:+ResizeTLAB -XX:+PerfDisableSharedMem -Djava.net.preferIPv4Stack=true -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1 -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSWaitDuration=10000 -XX:+CMSParallelInitialMarkEnabled -XX:+CMSEdenChunksRecordAlways -XX:+CMSClassUnloadingEnabled -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M -Djdk.nio.maxCachedBufferSize=1048576 -Xms8192M -Xmx8G -Xmn1600M -XX:CompileCommandFile=/etc/cassandra/conf/hotspot_compiler -javaagent:/usr/share/cassandra/lib/jamm-0.3.0.jar -Dcassandra.jmx.local.port=7199 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password -Djava.library.path=/usr/share/cassandra/lib/sigar-bin -Dcassandra.libjemalloc=/usr/lib64/libjemalloc.so.1 -Dlogback.configurationFile=logback.xml -Dcassandra.logdir=/var/log/cassandra -Dcassandra.storagedir= -Dcassandra-pidfile=/var/run/cassandra/cassandra.pid -cp /etc/cassandra/conf:/usr/share/cassandra/lib/airline-0.6.jar:/usr/share/cassandra/lib/antlr-runtime-3.5.2.jar:/usr/share/cassandra/lib/asm-5.0.4.jar:/usr/share/cassandra/lib/caffeine-2.2.6.jar:/usr/share/cassandra/lib/cassandra-driver-core-3.0.1-shaded.jar:/usr/share/cassandra/lib/commons-cli-1.1.jar:/usr/share/cassandra/lib/commons-codec-1.2.jar:/usr/share/cassandra/lib/commons-lang3-3.1.jar:/usr/share/cassandra/lib/commons-math3-3.2.jar:/usr/share/cassandra/lib/compress-lzf-0.8.4.jar:/usr/share/cassandra/lib/concurrentlinkedhashmap-lru-1.4.jar:/usr/share/cassandra/lib/concurrent-trees-2.4.0.jar:/usr/share/cassandra/lib/disruptor-3.0.1.jar:/usr/share/cassandra/lib/ecj-4.4.2.jar:/usr/share/cassandra/lib/guava-18.0.jar:/usr/share/cassandra/lib/HdrHistogram-2.1.9.jar:/usr/share/cassandra/lib/high-scale-lib-1.0.6.jar:/usr/share/cassandra/lib/hppc-0.5.4.jar:/usr/share/cassandra/lib/jackson-core-asl-1.9.2.jar:/usr/share/cassandra/lib/jackson-mapper-asl-1.9.2.jar:/usr/share/cassandra/lib/jamm-0.3.0.jar:/usr/share/cassandra/lib/javax.inject.jar:/usr/share/cassandra/lib/jbcrypt-0.3m.jar:/usr/share/cassandra/lib/jcl-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/jflex-1.6.0.jar:/usr/share/cassandra/lib/jna-4.0.0.jar:/usr/share/cassandra/lib/joda-time-2.4.jar:/usr/share/cassandra/lib/json-simple-1.1.jar:/usr/share/cassandra/lib/libthrift-0.9.2.jar:/usr/share/cassandra/lib/log4j-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/logback-classic-1.1.3.jar:/usr/share/cassandra/lib/logback-core-1.1.3.jar:/usr/share/cassandra/lib/lz4-1.3.0.jar:/usr/share/cassandra/lib/metrics-core-3.1.0.jar:/usr/share/cassandra/lib/metrics-jvm-3.1.0.jar:/usr/share/cassandra/lib/metrics-logback-3.1.0.jar:/usr/share/cassandra/lib/netty-all-4.0.39.Final.jar:/usr/share/cassandra/lib/ohc-core-0.4.3.jar:/usr/share/cassandra/lib/ohc-core-j8-0.4.3.jar:/usr/share/cassandra/lib/primitive-1.0.jar:/usr/share/cassandra/lib/reporter-config3-3.0.0.jar:/usr/share/cassandra/lib/reporter-config-base-3.0.0.jar:/usr/share/cassandra/lib/sigar-1.6.4.jar:/usr/share/cassandra/lib/slf4j-api-1.7.7.jar:/usr/share/cassandra/lib/snakeyaml-1.11.jar:/usr/share/cassandra/lib/snappy-java-1.1.1.7.jar:/usr/share/cassandra/lib/snowball-stemmer-1.3.0.581.1.jar:/usr/share/cassandra/lib/ST4-4.0.8.jar:/usr/share/cassandra/lib/stream-2.5.2.jar:/usr/share/cassandra/lib/thrift-server-0.3.7.jar:/usr/share/cassandra/apache-cassandra-3.9.0.jar:/usr/share/cassandra/apache-cassandra-thrift-3.9.0.jar:/usr/share/cassandra/stress.jar: org.apache.cassandra.service.CassandraDaemon\n"
        /* cassandra command formatted for readability
        cassand+ 13320  220 13.9 65608228 18399264 ?   S<Ll Sep17 59884:48
        /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/bin/java
        -Xloggc:/var/log/cassandra/gc.log
        -ea
        -XX:+UseThreadPriorities
        -XX:ThreadPriorityPolicy=42
        -XX:+HeapDumpOnOutOfMemoryError
        -Xss256k
        -XX:StringTableSize=1000003
        -XX:+AlwaysPreTouch
        -XX:-UseBiasedLocking
        -XX:+UseTLAB
        -XX:+ResizeTLAB
        -XX:+PerfDisableSharedMem
        -Djava.net.preferIPv4Stack=true
        -XX:+UseParNewGC
        -XX:+UseConcMarkSweepGC
        -XX:+CMSParallelRemarkEnabled
        -XX:SurvivorRatio=8
        -XX:MaxTenuringThreshold=1
        -XX:CMSInitiatingOccupancyFraction=75
        -XX:+UseCMSInitiatingOccupancyOnly
        -XX:CMSWaitDuration=10000
        -XX:+CMSParallelInitialMarkEnabled
        -XX:+CMSEdenChunksRecordAlways
        -XX:+CMSClassUnloadingEnabled
        -XX:+PrintGCDetails
        -XX:+PrintGCDateStamps
        -XX:+PrintHeapAtGC
        -XX:+PrintTenuringDistribution
        -XX:+PrintGCApplicationStoppedTime
        -XX:+PrintPromotionFailure
        -XX:+UseGCLogFileRotation
        -XX:NumberOfGCLogFiles=10
        -XX:GCLogFileSize=10M
        -Djdk.nio.maxCachedBufferSize=1048576
        -Xms8192M
        -Xmx8G
        -Xmn1600M
        -XX:CompileCommandFile=/etc/cassandra/conf/hotspot_compiler
        -javaagent:/usr/share/cassandra/lib/jamm-0.3.0.jar
        -Dcassandra.jmx.local.port=7199
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password
        -Djava.library.path=/usr/share/cassandra/lib/sigar-bin
        -Dcassandra.libjemalloc=/usr/lib64/libjemalloc.so.1
        -Dlogback.configurationFile=logback.xml
        -Dcassandra.logdir=/var/log/cassandra
        -Dcassandra.storagedir=
        -Dcassandra-pidfile=/var/run/cassandra/cassandra.pid
        -cp /etc/cassandra/conf:/usr/share/cassandra/lib/airline-0.6.jar:/usr/share/cassandra/lib/antlr-runtime-3.5.2.jar:/usr/share/cassandra/lib/asm-5.0.4.jar:/usr/share/cassandra/lib/caffeine-2.2.6.jar:/usr/share/cassandra/lib/cassandra-driver-core-3.0.1-shaded.jar:/usr/share/cassandra/lib/commons-cli-1.1.jar:/usr/share/cassandra/lib/commons-codec-1.2.jar:/usr/share/cassandra/lib/commons-lang3-3.1.jar:/usr/share/cassandra/lib/commons-math3-3.2.jar:/usr/share/cassandra/lib/compress-lzf-0.8.4.jar:/usr/share/cassandra/lib/concurrentlinkedhashmap-lru-1.4.jar:/usr/share/cassandra/lib/concurrent-trees-2.4.0.jar:/usr/share/cassandra/lib/disruptor-3.0.1.jar:/usr/share/cassandra/lib/ecj-4.4.2.jar:/usr/share/cassandra/lib/guava-18.0.jar:/usr/share/cassandra/lib/HdrHistogram-2.1.9.jar:/usr/share/cassandra/lib/high-scale-lib-1.0.6.jar:/usr/share/cassandra/lib/hppc-0.5.4.jar:/usr/share/cassandra/lib/jackson-core-asl-1.9.2.jar:/usr/share/cassandra/lib/jackson-mapper-asl-1.9.2.jar:/usr/share/cassandra/lib/jamm-0.3.0.jar:/usr/share/cassandra/lib/javax.inject.jar:/usr/share/cassandra/lib/jbcrypt-0.3m.jar:/usr/share/cassandra/lib/jcl-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/jflex-1.6.0.jar:/usr/share/cassandra/lib/jna-4.0.0.jar:/usr/share/cassandra/lib/joda-time-2.4.jar:/usr/share/cassandra/lib/json-simple-1.1.jar:/usr/share/cassandra/lib/libthrift-0.9.2.jar:/usr/share/cassandra/lib/log4j-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/logback-classic-1.1.3.jar:/usr/share/cassandra/lib/logback-core-1.1.3.jar:/usr/share/cassandra/lib/lz4-1.3.0.jar:/usr/share/cassandra/lib/metrics-core-3.1.0.jar:/usr/share/cassandra/lib/metrics-jvm-3.1.0.jar:/usr/share/cassandra/lib/metrics-logback-3.1.0.jar:/usr/share/cassandra/lib/netty-all-4.0.39.Final.jar:/usr/share/cassandra/lib/ohc-core-0.4.3.jar:/usr/share/cassandra/lib/ohc-core-j8-0.4.3.jar:/usr/share/cassandra/lib/primitive-1.0.jar:/usr/share/cassandra/lib/reporter-config3-3.0.0.jar:/usr/share/cassandra/lib/reporter-config-base-3.0.0.jar:/usr/share/cassandra/lib/sigar-1.6.4.jar:/usr/share/cassandra/lib/slf4j-api-1.7.7.jar:/usr/share/cassandra/lib/snakeyaml-1.11.jar:/usr/share/cassandra/lib/snappy-java-1.1.1.7.jar:/usr/share/cassandra/lib/snowball-stemmer-1.3.0.581.1.jar:/usr/share/cassandra/lib/ST4-4.0.8.jar:/usr/share/cassandra/lib/stream-2.5.2.jar:/usr/share/cassandra/lib/thrift-server-0.3.7.jar:/usr/share/cassandra/apache-cassandra-3.9.0.jar:/usr/share/cassandra/apache-cassandra-thrift-3.9.0.jar:/usr/share/cassandra/stress.jar:
        org.apache.cassandra.service.CassandraDaemon\n"
         */

        val jvmSettings = JVMSettingsParser.parse(CASSANDRA_COMMAND)
        assertThat(jvmSettings.gcAlgorithm.name).isEqualTo(GCAlgorithm.CMS.name)
        assertEquals("8.0 GiB", ByteCountHelper.humanReadableByteCount(jvmSettings.heapSize, ByteCountHelperUnits.BINARY))
        assertEquals("1.56 GiB", ByteCountHelper.humanReadableByteCount(jvmSettings.newGenSize!!, ByteCountHelperUnits.BINARY))
        assertThat(jvmSettings.gcFlags.size).isEqualTo(31)
        assertThat(jvmSettings.gcFlags.contains("-Xloggc:/var/log/cassandra/gc.log")).isTrue()
        assertThat(jvmSettings.gcFlags.contains("-XX:+CMSParallelRemarkEnabled")).isTrue()

    }


    @Test
    fun testParseJvmSettingsCMS() {
        val commandLine = "cassand+ 32330  124 41.0 568849124 21883976 ?  SLl  May14 4174:40 java -Dcassandra.initial_token=-9223372036854775802 -ea -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -Xloggc:/var/log/cassandra/gc.log -Xmx8G -Xms8G -Xmn800M -XX:HeapDumpPath=/spotify/tmp/ -Xss256k -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+ExplicitGCInvokesConcurrent -javaagent:/usr/share/cassandra/lib/jamm-0.3.0.jar -Djava.library.path=/usr/lib/jni -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -XX:+UseTLAB -Dlogback.configurationFile=logback.xml -Dcassandra.logdir=/var/log/cassandra -Dcassandra.storagedir= -Dcassandra-pidfile=/var/run/cassandra/cassandra.pid -cp /etc/cassandra:/usr/share/cassandra/lib/ST4-4.0.8.jar:/usr/share/cassandra/lib/airline-0.6.jar:/usr/share/cassandra/lib/antlr-runtime-3.5.2.jar:/usr/share/cassandra/lib/cassandra-driver-core-2.2.0-rc2-SNAPSHOT-20150617-shaded.jar:/usr/share/cassandra/lib/commons-cli-1.1.jar:/usr/share/cassandra/lib/commons-codec-1.2.jar:/usr/share/cassandra/lib/commons-lang3-3.1.jar:/usr/share/cassandra/lib/commons-math3-3.2.jar:/usr/share/cassandra/lib/compress-lzf-0.8.4.jar:/usr/share/cassandra/lib/concurrentlinkedhashmap-lru-1.4.jar:/usr/share/cassandra/lib/crc32ex-0.1.1.jar:/usr/share/cassandra/lib/disruptor-3.0.1.jar:/usr/share/cassandra/lib/ecj-4.4.2.jar:/usr/share/cassandra/lib/guava-16.0.jar:/usr/share/cassandra/lib/hecuba2-seedprovider-1.3.4.jar:/usr/share/cassandra/lib/high-scale-lib-1.0.6.jar:/usr/share/cassandra/lib/jackson-core-asl-1.9.2.jar:/usr/share/cassandra/lib/jackson-mapper-asl-1.9.2.jar:/usr/share/cassandra/lib/jamm-0.3.0.jar:/usr/share/cassandra/lib/javax.inject.jar:/usr/share/cassandra/lib/jbcrypt-0.3m.jar:/usr/share/cassandra/lib/jcl-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/jna-4.0.0.jar:/usr/share/cassandra/lib/joda-time-2.4.jar:/usr/share/cassandra/lib/json-simple-1.1.jar:/usr/share/cassandra/lib/json-smart-1.1.1.jar:/usr/share/cassandra/lib/jsonevent-layout-1.7-spotify1.jar:/usr/share/cassandra/lib/libthrift-0.9.2.jar:/usr/share/cassandra/lib/log4j-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/logback-classic-1.1.3.jar:/usr/share/cassandra/lib/logback-core-1.1.3.jar:/usr/share/cassandra/lib/lz4-1.3.0.jar:/usr/share/cassandra/lib/metrics-core-3.1.0.jar:/usr/share/cassandra/lib/metrics-jvm-3.1.0.jar:/usr/share/cassandra/lib/metrics-logback-3.1.0.jar:/usr/share/cassandra/lib/netty-all-4.0.44.Final.jar:/usr/share/cassandra/lib/ohc-core-0.3.4.jar:/usr/share/cassandra/lib/ohc-core-j8-0.3.4.jar:/usr/share/cassandra/lib/reporter-config-base-3.0.0.jar:/usr/share/cassandra/lib/reporter-config3-3.0.0.jar:/usr/share/cassandra/lib/sigar-1.6.4.jar:/usr/share/cassandra/lib/slf4j-api-1.7.7.jar:/usr/share/cassandra/lib/snakeyaml-1.11.jar:/usr/share/cassandra/lib/snappy-java-1.1.1.7.jar:/usr/share/cassandra/lib/snitch.jar:/usr/share/cassandra/lib/stream-2.5.2.jar:/usr/share/cassandra/lib/super-csv-2.1.0.jar:/usr/share/cassandra/lib/thrift-server-0.3.7.jar:/usr/share/cassandra/apache-cassandra-2.2.9.jar:/usr/share/cassandra/apache-cassandra-thrift-2.2.9.jar:/usr/share/cassandra/apache-cassandra.jar:/usr/share/cassandra/ic-sstable-tools.jar:/usr/share/cassandra/stress.jar: -XX:HeapDumpPath=/var/lib/cassandra/java_1526286446.hprof -XX:ErrorFile=/var/lib/cassandra/hs_err_1526286446.log org.apache.cassandra.service.CassandraDaemon"
        val gcSettings = JVMSettingsParser.parse(commandLine)
        assertEquals(gcSettings.gcAlgorithm.name, GCAlgorithm.CMS.name)
        assertEquals("8.0 GiB", ByteCountHelper.humanReadableByteCount(gcSettings.heapSize, ByteCountHelperUnits.BINARY))
        assertEquals("800.0 MiB", ByteCountHelper.humanReadableByteCount(gcSettings.newGenSize!!, ByteCountHelperUnits.BINARY))
        assertTrue(gcSettings.gcFlags.contains("-XX:+CMSParallelRemarkEnabled"))
        assertTrue(gcSettings.gcFlags.contains("-Xloggc:/var/log/cassandra/gc.log"))
    }

    @Test
    fun testParseJvmSettingsG1() {
        val commandLine = "cassand+  2893 82.7 64.1 684880524 68658540 ?  SLl  Jun12 40563:16 java -ea -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -Xloggc:/var/log/cassandra/gc.log -Xmx31G -Xms31G -XX:HeapDumpPath=/spotify/tmp/ -Xss256k -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=0 -XX:+UseG1GC -XX:G1RSetUpdatingPauseTimePercent=10 -javaagent:/usr/share/cassandra/lib/jamm-0.3.0.jar -Djava.library.path=/usr/lib/jni -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -XX:+UseTLAB -XX:NewSize=2500m -XX:MaxGCPauseMillis=300 -XX:G1HeapRegionSize=16m -XX:+UnlockExperimentalVMOptions -XX:ParallelGCThreads=16 -XX:ConcGCThreads=4 -XX:+AlwaysPreTouch -XX:-UseBiasedLocking -XX:+ParallelRefProcEnabled -Dlogback.configurationFile=logback.xml -Dcassandra.logdir=/var/log/cassandra -Dcassandra.storagedir= -Dcassandra-pidfile=/var/run/cassandra/cassandra.pid -cp /etc/cassandra:/usr/share/cassandra/lib/ST4-4.0.8.jar:/usr/share/cassandra/lib/airline-0.6.jar:/usr/share/cassandra/lib/antlr-runtime-3.5.2.jar:/usr/share/cassandra/lib/cassandra-driver-core-2.2.0-rc2-SNAPSHOT-20150617-shaded.jar:/usr/share/cassandra/lib/commons-cli-1.1.jar:/usr/share/cassandra/lib/commons-codec-1.2.jar:/usr/share/cassandra/lib/commons-lang3-3.1.jar:/usr/share/cassandra/lib/commons-math3-3.2.jar:/usr/share/cassandra/lib/compress-lzf-0.8.4.jar:/usr/share/cassandra/lib/concurrentlinkedhashmap-lru-1.4.jar:/usr/share/cassandra/lib/crc32ex-0.1.1.jar:/usr/share/cassandra/lib/disruptor-3.0.1.jar:/usr/share/cassandra/lib/ecj-4.4.2.jar:/usr/share/cassandra/lib/guava-16.0.jar:/usr/share/cassandra/lib/hecuba2-seedprovider-1.3.4.jar:/usr/share/cassandra/lib/high-scale-lib-1.0.6.jar:/usr/share/cassandra/lib/jackson-core-asl-1.9.2.jar:/usr/share/cassandra/lib/jackson-mapper-asl-1.9.2.jar:/usr/share/cassandra/lib/jamm-0.3.0.jar:/usr/share/cassandra/lib/javax.inject.jar:/usr/share/cassandra/lib/jbcrypt-0.3m.jar:/usr/share/cassandra/lib/jcl-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/jna-4.0.0.jar:/usr/share/cassandra/lib/joda-time-2.4.jar:/usr/share/cassandra/lib/json-simple-1.1.jar:/usr/share/cassandra/lib/json-smart-1.1.1.jar:/usr/share/cassandra/lib/jsonevent-layout-1.7-spotify1.jar:/usr/share/cassandra/lib/libthrift-0.9.2.jar:/usr/share/cassandra/lib/log4j-over-slf4j-1.7.7.jar:/usr/share/cassandra/lib/logback-classic-1.1.3.jar:/usr/share/cassandra/lib/logback-core-1.1.3.jar:/usr/share/cassandra/lib/lz4-1.3.0.jar:/usr/share/cassandra/lib/metrics-core-3.1.0.jar:/usr/share/cassandra/lib/metrics-jvm-3.1.0.jar:/usr/share/cassandra/lib/metrics-logback-3.1.0.jar:/usr/share/cassandra/lib/netty-all-4.0.44.Final.jar:/usr/share/cassandra/lib/ohc-core-0.3.4.jar:/usr/share/cassandra/lib/ohc-core-j8-0.3.4.jar:/usr/share/cassandra/lib/reporter-config-base-3.0.0.jar:/usr/share/cassandra/lib/reporter-config3-3.0.0.jar:/usr/share/cassandra/lib/sigar-1.6.4.jar:/usr/share/cassandra/lib/slf4j-api-1.7.7.jar:/usr/share/cassandra/lib/snakeyaml-1.11.jar:/usr/share/cassandra/lib/snappy-java-1.1.1.7.jar:/usr/share/cassandra/lib/snitch.jar:/usr/share/cassandra/lib/stream-2.5.2.jar:/usr/share/cassandra/lib/super-csv-2.1.0.jar:/usr/share/cassandra/lib/thrift-server-0.3.7.jar:/usr/share/cassandra/apache-cassandra-2.2.9.jar:/usr/share/cassandra/apache-cassandra-thrift-2.2.9.jar:/usr/share/cassandra/apache-cassandra.jar:/usr/share/cassandra/stress.jar: -XX:HeapDumpPath=/var/lib/cassandra/java_1528809934.hprof -XX:ErrorFile=/var/lib/cassandra/hs_err_1528809934.log org.apache.cassandra.service.CassandraDaemon"
        val gcSettings = JVMSettingsParser.parse(commandLine)
        assertEquals(gcSettings.gcAlgorithm, GCAlgorithm.G1GC)
        assertEquals("31.0 GiB", ByteCountHelper.humanReadableByteCount(gcSettings.heapSize, ByteCountHelperUnits.BINARY))
        assertEquals("2.44 GiB", ByteCountHelper.humanReadableByteCount(gcSettings.newGenSize!!, ByteCountHelperUnits.BINARY))
        assertTrue(gcSettings.gcFlags.contains("-XX:SurvivorRatio=8"))
        assertTrue(gcSettings.gcFlags.contains("-Xloggc:/var/log/cassandra/gc.log"))
        assertEquals(25, gcSettings.gcFlags.size)

    }

    @Test
    fun testParseJVMSettingsXTX() {
        val line = "cassand+ 30156  225 59.8 58187184 39408940 ?   SLl  Apr06 132339:12 /usr/local/java/jdk1.8.0_45/bin/java -ea -javaagent:/usr/share/cassandra/lib/jamm-0.3.0.jar -XX:+CMSClassUnloadingEnabled -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -Xms30500m -Xmx30500m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/cassandra-dump/cassandra-1523021868-pid30043.hprof -Xss256k -XX:StringTableSize=1000003 -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1RSetUpdatingPauseTimePercent=5 -XX:InitiatingHeapOccupancyPercent=25 -XX:+UseStringDeduplication -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1 -XX:+UseTLAB -XX:+ResizeTLAB -XX:CompileCommandFile=/etc/cassandra/conf/hotspot_compiler -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch -XX:-UseBiasedLocking -XX:+CMSParallelInitialMarkEnabled -XX:+CMSEdenChunksRecordAlways -XX:CMSWaitDuration=10000 -XX:+UseCondCardMark -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:PrintFLSStatistics=1 -Xloggc:/var/log/cassandra/gc-1523021868.log -Xloggc:/var/log/cassandra/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=10M -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.rmi.port=7199 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dlogback.configurationFile=logback.xml -Dcassandra.logdir=/var/log/cassandra -Dcassandra.storagedir= -Dcassandra-pidfile=/var/run/cassandra/cassandra.pid -cp /etc/cassandra/conf:/usr/share/cassandra/lib/airline-0.6.jar:/usr/share/cassandra/lib/antlr-runtime-3.5.2.jar:/usr/share/cassandra/lib/commons-cli-1.1.jar:/usr/share/cassandra/lib/commons-codec-1.2.jar:/usr/share/cassandra/lib/commons-lang3-3.1.jar:/usr/share/cassandra/lib/commons-math3-3.2.jar:/usr/share/cassandra/lib/compress-lzf-0.8.4.jar:/usr/share/cassandra/lib/concurrentlinkedhashmap-lru-1.4.jar:/usr/share/cassandra/lib/disruptor-3.0.1.jar:/usr/share/cassandra/lib/guava-16.0.jar:/usr/share/cassandra/lib/high-scale-lib-1.0.6.jar:/usr/share/cassandra/lib/jackson-core-asl-1.9.2.jar:/usr/share/cassandra/lib/jackson-mapper-asl-1.9.2.jar:/usr/share/cassandra/lib/jamm-0.3.0.jar:/usr/share/cassandra/lib/javax.inject.jar:/usr/share/cassandra/lib/jbcrypt-0.3m.jar:/usr/share/cassandra/lib/jline-1.0.jar:/usr/share/cassandra/lib/jna-4.0.0.jar:/usr/share/cassandra/lib/json-simple-1.1.jar:/usr/share/cassandra/lib/libthrift-0.9.2.jar:/usr/share/cassandra/lib/logback-classic-1.1.2.jar:/usr/share/cassandra/lib/logback-core-1.1.2.jar:/usr/share/cassandra/lib/lz4-1.2.0.jar:/usr/share/cassandra/lib/metrics-core-2.2.0.jar:/usr/share/cassandra/lib/netty-all-4.0.23.Final.jar:/usr/share/cassandra/lib/reporter-config-2.1.0.jar:/usr/share/cassandra/lib/slf4j-api-1.7.2.jar:/usr/share/cassandra/lib/snakeyaml-1.11.jar:/usr/share/cassandra/lib/snappy-java-1.0.5.2.jar:/usr/share/cassandra/lib/ST4-4.0.8.jar:/usr/share/cassandra/lib/stream-2.5.2.jar:/usr/share/cassandra/lib/super-csv-2.1.0.jar:/usr/share/cassandra/lib/thrift-server-0.3.7.jar:/usr/share/cassandra/apache-cassandra-2.1.13.jar:/usr/share/cassandra/apache-cassandra-thrift-2.1.13.jar:/usr/share/cassandra/cassandra-driver-core-2.0.9.2.jar:/usr/share/cassandra/netty-3.9.0.Final.jar:/usr/share/cassandra/stress.jar: org.apache.cassandra.service.CassandraDaemon"
        val gcSettings = JVMSettingsParser.parse(line)
    }

    @Test
    fun testParseHeapSize() {
        assertEquals(31L * 1024L * 1024L * 1024L, JVMSettingsParser.parseHeapSize("31G"))
        assertEquals(31L * 1024L * 1024L * 1024L, JVMSettingsParser.parseHeapSize("31g"))
        assertEquals(30500L * 1024 * 1024, JVMSettingsParser.parseHeapSize("30500m"))
        assertEquals(30500L * 1024 * 1024, JVMSettingsParser.parseHeapSize("30500M"))
    }

    @Test
    fun testParseHeapSettingsOpsCenterTarball() {
        val jvmSettings = "-XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42 -XX:+HeapDumpOnOutOfMemoryError -Xss256k -XX:StringTableSize=1000003 -XX:+AlwaysPreTouch -XX:-UseBiasedLocking -XX:+UseTLAB -XX:+ResizeTLAB -XX:+UseNUMA -XX:+PerfDisableSharedMem -XX:+UseG1GC -XX:G1RSetUpdatingPauseTimePercent=5 -XX:MaxGCPauseMillis=500"
        val parsed =JVMSettingsParser.parse(jvmSettings)
        assertEquals( GCAlgorithm.G1GC, parsed.gcAlgorithm)
    }
}