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
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.search.*
import org.apache.lucene.store.Directory
import java.time.LocalDateTime


class Searcher(directory: Directory, var daysIncluded : Long, private var minDatesPerNode: Map<String, LocalDateTime>) {

    private val dir: DirectoryReader = DirectoryReader.open(directory)
    val searcher = IndexSearcher(dir)

    /**
     * This uses the absurdly powerful classic lucene query library
     * Some examples, so you don't have to read the link:
     *
     * Search by level, containing repair:
     * query("message:repair")
     *
     * Search for a phrase:
     * query(""" message:"tombstone cells for query" """)
     *
     * Multiple terms in a message:
     * query(""" message:(+"live rows" + "tombstone cells for query") """)
     *
     * https://lucene.apache.org/core/7_2_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description
     * https://lucene.apache.org/core/7_2_1/queryparser/index.html
     */
    fun search(
        luceneQuery: String,
        level: LogLevel = LogLevel.TRACE,
        limit: Int = 100
    ): List<LogEntry> {
        val result = mutableListOf<LogEntry>()
        val parser = StandardQueryParser()
        parser.analyzer = StandardAnalyzer()

        // if we're going to support searching by log level i need to use a non tokenizing / non stemming analyzer
        // one that just takes raw tokens
        val query = parser.parse(luceneQuery, "message")

        val builder = BooleanQuery.Builder()
        builder.add(query, BooleanClause.Occur.MUST)

        if (level != LogLevel.TRACE) {
            val tq = TermQuery(Term("level", level.toString()))
            builder.add(tq, BooleanClause.Occur.MUST)
        }
        println("Lucene Query : ${builder.build()}")
        // we always want the latest stuff first
        val docs = searcher.search(builder.build(), limit, Sort(SortField("timestamp", SortField.Type.STRING, true)))
        var countIncluded = 0
        for (doc in docs.scoreDocs) {
            // have to get the original doc out of the index, we only get scores in TopDocs
            val tmp = searcher.doc(doc.doc)
            val level = tmp.get("level")
            val message = tmp.get("message")
            val logTimestamp = tmp.get("timestamp")
            val node = tmp.get("host")
            val entry = LogEntry(
                level, message, logTimestamp, node
            )

            val minForThisNode = minDatesPerNode[node]
            if (entry.getDate().isAfter(minForThisNode)) {
                result.add(entry)
                countIncluded++
            }
        }
        println("Found : ${docs.totalHits} , Included : $countIncluded")
        return result
    }


}
