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

package com.datastax.montecristo.logs.logMessageParsers

import com.datastax.montecristo.logs.LogEntry
import com.datastax.montecristo.model.os.GCAlgorithm
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test

class GcPauseMessageTest {
    @Test
    fun testParseParNewGCTime() {
        val example = "GCInspector.java:284 - ParNew GC in 237ms.  CMS Old Gen: 6661992176 -> 6706569168; Par Eden Space: 671088640 -> 0; Par Survivor Space: 64102592 -> 66356720"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")
        val pause = GCPauseMessage.fromLogEntry(entry)
        val date = entry.getDate()
        assertNotNull(pause)
        assertEquals(GCAlgorithm.PARNEW, pause?.algorithm)
        assertEquals(237, pause?.timeInMS)
    }

    @Test
    fun testCMSTime() {

        val example = "GCInspector.java:284 - ConcurrentMarkSweep GC in 257ms.  CMS Old Gen: 9040926072 -> 1837398184; Code Cache: 42171840 -> 42174208; Par Eden Space: 321144 -> 280697744;"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")

        val pause = GCPauseMessage.fromLogEntry(entry)
        val date = entry.getDate()
        assertNotNull(pause)
        assertEquals(GCAlgorithm.CMS, pause?.algorithm)
        assertEquals(257, pause?.timeInMS)

    }
    @Test
    fun testG1GC() {
        val example = "GCInspector.java (line 116) GC for G1 Young Generation: 523 ms for 1 collections, 8233605112 used; max is 17389584384"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")
        val tmp = GCPauseMessage.fromLogEntry(entry)
        assertNotNull(tmp)
    }

    // not sure what the deal here is, but it popped up.  i think's an older version of the log message from 2.0
    @Test
    fun testCMSOld() {
        val example = "GCInspector.java (line 116) GC for ConcurrentMarkSweep: 237 ms for 1 collections, 6636463120 used; max is 8327790592"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")
        val tmp = GCPauseMessage.fromLogEntry(entry)
        assertNotNull(tmp)
    }

    @Test
    fun testParNewOld() {
        val example = "-0700 GCInspector.java (line 116) GC for ParNew: 230 ms for 1 collections, 4217037032 used; max is 8327790592"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")
        val tmp = GCPauseMessage.fromLogEntry(entry)
        assertNotNull(tmp)

    }

    @Test
    fun testParallelMarkSweep() {
        val example = "+0000 GCInspector.java (line 116) GC for PS MarkSweep: 234 ms for 1 collections, 134543864 used; max is 14310965248"
        val entry = LogEntry("INFO", example, "20180214091505", "myhost")
        val tmp = GCPauseMessage.fromLogEntry(entry)
        assertNotNull(tmp)

    }
}