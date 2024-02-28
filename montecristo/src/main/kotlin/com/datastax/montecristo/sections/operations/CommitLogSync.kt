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

package com.datastax.montecristo.sections.operations

import com.datastax.montecristo.helpers.toHumanCount
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.logs.logMessageParsers.CommitLogSyncMessage
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near
import com.datastax.montecristo.utils.MarkdownTable
import kotlin.math.roundToLong

class CommitLogSync : DocumentSection {

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        // search and parse the commit log sync messages
        val commitWarnings = logSearcher.search(
            "PERIODIC-COMMIT-LOG-SYNC OR PERIODIC-COMMIT-LOG-SYNCER",
            limit = LIMIT
        ).mapNotNull { CommitLogSyncMessage.fromLogEntry(it) }

        // so now what
        // we have pauses, lets get some simple statistics
        // first - did we hit the limit (and thus need to caveat any figures)
        val hitMessageLimit = commitWarnings.size == LIMIT // did we hit the search limit?

        // Count of messages per node
        val countOfMessagesPerNode = commitWarnings.groupingBy { it.host }.eachCount()
        val countOfExceedingPerNode = commitWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.sumOf { it.numberExceeding } }
        val averageTimeExceedingPerNode = commitWarnings.groupBy { node -> node.host }.mapValues { message -> message.value.map { it.averageExceededCommitInterval }.average() }

        // average of the exceeding time by node
        val countPerNodeTable = MarkdownTable("Host", "Number of Messages", "Syncs Exceeding Period", "Average Time Exceeded (ms)")
        countOfMessagesPerNode.forEach {
            countPerNodeTable.addRow()
                    .addField(it.key)
                    .addField(it.value)
                    .addField( (countOfExceedingPerNode[it.key] ?:0 ).toLong().toHumanCount())
                    .addField((averageTimeExceedingPerNode[it.key] ?: 0.0).roundToLong())
        }

        val countOfWarningsMessage =  if (hitMessageLimit) {
            "More than $LIMIT commit log sync warnings were discovered within the logs"
        } else {
            "A total of ${commitWarnings.size} commit log sync warnings were discovered within the logs."
        }

        if (commitWarnings.size > MIN_WARNINGS_TO_RECOMMEND) {
            recs.near(RecommendationType.OPERATIONS,"We recommend further investigation into the commit log sync warnings.")
        }

        args["countOfWarnings"] = countOfWarningsMessage
        args["commitLogSyncMessagesTable"] = countPerNodeTable.toString()
        return compileAndExecute("operations/operations_commit_log_sync.md", args)
    }

    companion object {
        private const val LIMIT = 1000000
        private const val MIN_WARNINGS_TO_RECOMMEND = 100
    }
}
