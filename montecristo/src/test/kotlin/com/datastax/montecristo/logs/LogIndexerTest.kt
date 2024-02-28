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

package com.datastax.montecristo.logs

import com.datastax.montecristo.model.logs.LogLevel
import org.apache.lucene.store.Directory
import org.apache.lucene.store.RAMDirectory
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.io.InputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun validate(searcher : Searcher, query: String, level: LogLevel, expected: Int) {
    val results = searcher.search("Writing large partition", level)
    assertThat(results.size).isEqualTo(expected)
}

class LogIndexerTest {

    private val m = "INFO  [main] 2017-11-14 13:24:39,127 DatabaseDescriptor.java:367 - DiskAccessMode 'auto' determined to be mmap, indexAccessMode is mmap"
    lateinit var ram : Directory

    lateinit var reader: InputStream

    @Before
    fun setup() {
        reader = this.javaClass.getResource("/system.example.log").openStream()
        ram = RAMDirectory()
    }

    @Test
    fun testParsingSimple() {
        // format: 20180307042938
        val regex = LogRegex.convert("%-5level [%thread] %date{ISO8601} %F:%L - %msg%n")
        val parsed = LogEntry.fromString(m, regex)
        assertThat( parsed.level).isEqualTo("INFO")
    }

    @Test
    fun testMultiLineParse() {
        val lines = """INFO  [STREAM-IN-/172.18.107.181:58867] 2018-02-14 09:22:24,375 StreamResultFuture.java:188 - [Stream #e64edb40-119f-11e8-a657-c5d31677e7b5] Session with /172.18.107.181 is complete
ERROR [STREAM-OUT-/172.18.107.181:36154] 2018-02-14 09:22:24,375 StreamSession.java:533 - [Stream #e64edb40-119f-11e8-a657-c5d31677e7b5] Streaming error occurred on session with peer 172.18.107.181
org.apache.cassandra.io.FSReadError: java.io.IOException: Broken pipe
        at org.apache.cassandra.io.util.ChannelProxy.transferTo(ChannelProxy.java:145) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.compress.CompressedStreamWriter.lambda${'$'}write${'$'}0(CompressedStreamWriter.java:90) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.io.util.BufferedDataOutputStreamPlus.applyToChannel(BufferedDataOutputStreamPlus.java:350) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.compress.CompressedStreamWriter.write(CompressedStreamWriter.java:90) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.messages.OutgoingFileMessage.serialize(OutgoingFileMessage.java:102) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.messages.OutgoingFileMessage${'$'}1.serialize(OutgoingFileMessage.java:53) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.messages.OutgoingFileMessage${'$'}1.serialize(OutgoingFileMessage.java:42) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.messages.StreamMessage.serialize(StreamMessage.java:48) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.ConnectionHandler${'$'}OutgoingMessageHandler.sendMessage(ConnectionHandler.java:389) ~[apache-cassandra-3.9.jar:3.9]
        at org.apache.cassandra.streaming.ConnectionHandler${'$'}OutgoingMessageHandler.run(ConnectionHandler.java:361) ~[apache-cassandra-3.9.jar:3.9]
        at java.lang.Thread.run(Thread.java:745) [na:1.8.0_102]
Caused by: java.io.IOException: Broken pipe
        at sun.nio.ch.FileChannelImpl.transferTo0(Native Method) ~[na:1.8.0_102]
        at sun.nio.ch.FileChannelImpl.transferToDirectlyInternal(FileChannelImpl.java:428) ~[na:1.8.0_102]
        at sun.nio.ch.FileChannelImpl.transferToDirectly(FileChannelImpl.java:493) ~[na:1.8.0_102]
        at sun.nio.ch.FileChannelImpl.transferTo(FileChannelImpl.java:608) ~[na:1.8.0_102]
        at org.apache.cassandra.io.util.ChannelProxy.transferTo(ChannelProxy.java:141) ~[apache-cassandra-3.9.jar:3.9]
        ... 10 common frames omitted
WARN  [STREAM-IN-/172.18.107.181:58867] 2018-02-14 09:22:24,375 StreamResultFuture.java:215 - [Stream #e64edb40-119f-11e8-a657-c5d31677e7b5] Stream failed"""
        val entries = LogIndexer.getLogEntries (lines.byteInputStream(), LogRegex.defaultRegex())
        assertThat(entries.size).isEqualTo(3)
        val message = entries.elementAt(1).message!!
        assert(message.contains("ChannelProxy.transferTo"))

        assertThat(entries.count()).isEqualTo(3)
    }

    @Test
    fun testWriteToIndex() {
        val logEntries = LogIndexer.getLogEntries(reader, LogRegex.defaultRegex())
        val indexWriter = LogIndexer.getWriter(RAMDirectory())
        val result = LogIndexer.writeToIndex(indexWriter, logEntries, "test_host")
        // validate that the min / max also got picked up
        val internalFormat = "yyyyMMddHHmmss"
        val formatter = DateTimeFormatter.ofPattern(internalFormat)
        assertThat(result.minDate).isEqualTo(LocalDateTime.parse ("20180209060642", formatter))
        assertThat(result.maxDate).isEqualTo(LocalDateTime.parse ("20180211083113", formatter))

    }

    @Test
    fun testSimpleSearch() {
        val writer = LogIndexer.getWriter(ram)
        val logEntries = LogIndexer.getLogEntries(reader, LogRegex.defaultRegex())
        LogIndexer.writeToIndex(writer, logEntries, "test_host")

        writer.close()
        val searcher = Searcher(ram, 90, mapOf(Pair("test_host", LocalDateTime.of(2018,2,1,0,0,0))))
        val results = searcher.search("Writing large partition")
        assertThat(results.isNotEmpty()).isTrue

        validate(searcher,"tombstone", LogLevel.INFO, 0)
        validate(searcher,"tombstone", LogLevel.WARN, 7)
        validate(searcher, "tombstone", LogLevel.DEBUG, 0)
    }

    @Test
    fun testQuotedSearch() {
        val writer = LogIndexer.getWriter(ram)
        val logEntries = LogIndexer.getLogEntries (reader, LogRegex.defaultRegex())
        LogIndexer.writeToIndex(writer, logEntries, "test_host")
        writer.close()

        val searcher = Searcher(ram,90,  mapOf(Pair("test_host", LocalDateTime.of(2018,2,1,0,0,0))))
        val results2 = searcher.search("""message:"Writing large partition" level:WARN """)
        assertThat(results2.isNotEmpty()).isTrue

        val results3 = searcher.search("""message:"Writing large partition" """)
        assertThat(results3.isNotEmpty()).isTrue

    }

    @Test
    fun testSpecialLogFormat1() {
        val lines = "16 Jun 2020 04:00:22,644 ^[[33m[WARN]^[[m  (ReadStage-1) org.apache.cassandra.db.ReadCommand: Read 312 live rows and 5438 tombstone cells for query SELECT * FROM ks.table WHERE f1, f2, f3 = yadayadayada, 7, 8 LIMIT 10000 (see tombstone_warn_threshold)"
        assertThat( LogIndexer.getLogEntries(lines.byteInputStream(), LogRegex.defaultRegex()).toList().size).isEqualTo(1)
        val parsed = LogEntry.fromString(lines, LogRegex.defaultRegex())
        assertThat(parsed.level).isEqualTo("WARN")
        assertThat(parsed.message).contains("Read 312 live rows and 5438")
        assertThat("${parsed.getDate().year}-${parsed.getDate().monthValue}-${parsed.getDate().dayOfMonth}").isEqualTo("2020-6-16")
    }

    @Test
    fun testSpecialLogFormat2() {
        val lines = "  ERROR [RMI RenewClean-[127.0.0.1:42887]] 2019-10-26 00:54:13,793 CassandraDaemon.java:229 - Exception in thread Thread[RMI RenewClean-[127.0.0.1:42887],5,system]"
        assertThat( LogIndexer.getLogEntries(lines.byteInputStream(), LogRegex.defaultRegex()).toList().size).isEqualTo(1)
        val parsed = LogEntry.fromString(lines, LogRegex.defaultRegex())
        assertThat(parsed.level).isEqualTo("ERROR")
        assertThat(parsed.message).isEqualTo("CassandraDaemon.java:229 - Exception in thread Thread[RMI RenewClean-[127.0.0.1:42887],5,system]")
        assertThat(parsed.timestamp).isEqualTo("20191026005413")
    }
}