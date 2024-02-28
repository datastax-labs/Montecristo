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

package com.datastax.montecristo.fileLoaders.parsers.schema

import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.model.schema.Caching
import com.datastax.montecristo.model.schema.CompactionDetail
import com.datastax.montecristo.model.schema.Compression
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.cql3.QualifiedName
import org.apache.cassandra.cql3.selection.RawSelector
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement
import org.apache.cassandra.cql3.statements.schema.CreateViewStatement
import org.apache.cassandra.cql3.statements.schema.IndexTarget
import org.slf4j.LoggerFactory

/**
 * dataTypeString is temporary
 * @TODO create a wrapper so we don't manually check strings for "map" "list" "set", etc
 */
data class Field(val dataTypeString: String)

enum class Direction { ASC, DESC }

// clustering keys
typealias ClusteringKey = Pair<String, Direction>

class ParsedCreateTable(val name: String,
                        val fields: Map<String, Field>,
                        val partitionKeys: List<String>,
                        val clusteringKeys: List<ClusteringKey>,
                        val gcGraceSeconds: Int,
                        val readRepairChance: String,
                        val dclocalReadRepairChance: String,
                        val bloomFilterFPChance: Double,
                        val compaction: CompactionDetail,
                        val compression: Compression,
                        val caching: Caching,
                        val indexName: String,
                        val baseTableName: String) {


    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)

        fun fromCQL(cql: String): ParsedCreateTable {
            logger.debug("Parsing $cql")

            return when (val statement = Utils.parseCQL(cql)) {
                is CreateTableStatement.Raw -> fromCreateTableStatement(statement)
                is CreateViewStatement.Raw -> fromCreateViewStatement(statement)
                is CreateIndexStatement.Raw -> fromCreateIndexStatement(statement)
                else -> throw Exception("Not a table, not a view: $cql")
            }
        }

        private fun fromCreateViewStatement(parsed: CreateViewStatement.Raw): ParsedCreateTable {

            val partition = Utils.readFieldForce(parsed, "partitionKeyColumns") as List<ColumnIdentifier>
            val clustering = Utils.readFieldForce(parsed, "clusteringColumns") as List<ColumnIdentifier>

            val viewName = Utils.readFieldForce(parsed, "viewName").toString()
            val baseName = Utils.readFieldForce(parsed, "tableName").toString()

            val select: Map<Any, Any> =
                (Utils.readFieldForce(parsed, "rawColumns") as List<RawSelector>).associate { Pair(it, "UNKNOWN_TYPE") }

            val (partitionKeys, clusteringKeys, allFields) =
                    parsePrimaryKey(partition, clustering, select)

            val attributes = parsed.attrs
            val properties = Utils.readFieldForce(attributes, "properties") as HashMap<String, *>

            val gcGraceSeconds = (properties.getOrDefault("gc_grace_seconds", "-1") as String).toInt()
            val readRepairChance = properties.getOrDefault("read_repair_chance", "").toString()
            val dcLocalReadRepairChance = properties.getOrDefault("dclocal_read_repair_chance", "").toString()
            val bloomFilterFpChance = properties.getOrDefault("bloom_filter_fp_chance", "0.01").toString().toDouble()

            val (compaction, compression, caching) = parseComplexProperties(properties)

            return ParsedCreateTable(
                    viewName,
                    allFields,
                    partitionKeys,
                    clusteringKeys,
                    gcGraceSeconds,
                    readRepairChance,
                    dcLocalReadRepairChance,
                    bloomFilterFpChance,
                    compaction,
                    compression,
                    caching,
                    "",
                    baseName
            )
        }

        private fun fromCreateIndexStatement(parsed: CreateIndexStatement.Raw): ParsedCreateTable {
            val qualifiedIndexName = Utils.readFieldForce(parsed, "indexName") as QualifiedName
            val qualifiedName = Utils.readFieldForce(parsed, "tableName") as QualifiedName

            val partitionKeys = mutableListOf<String>()
            val partition = Utils.readFieldForce(parsed, "rawIndexTargets") as List<IndexTarget.Raw>
            for (part in partition) {
                val indexColumn = Utils.readFieldForce(part, "column") as ColumnIdentifier
                partitionKeys.add(Utils.readFieldForce(indexColumn, "text") as String)
            }

            val allFields = mutableMapOf<String, Field>()
            allFields[partitionKeys[0]] = Field("text")
            return ParsedCreateTable(
                    "${qualifiedName.keyspace}.${qualifiedName.name}",
                    emptyMap(),
                    partitionKeys,
                    emptyList(),
                    864000,
                    "0.0",
                    "0.01",
                    0.001,
                    CompactionDetail.none(),
                    emptyCompression(),
                    noCaching(),
                qualifiedIndexName.name,
                    ""
            )
        }

        private fun emptyCompression(): Compression {
            return Compression("none", HashMap())
        }

        /**
         * Parses CQL string and returns a ParsedCreateTable
         */
        private fun fromCreateTableStatement(parsed: CreateTableStatement.Raw): ParsedCreateTable {

            val keyAliases = Utils.readFieldForce(parsed, "partitionKeyColumns") as List<ColumnIdentifier>
            val columnAliases = Utils.readFieldForce(parsed, "clusteringColumns") as List<ColumnIdentifier>
            val definitions = Utils.readFieldForce(parsed, "rawColumns") as Map<Any, Any>
            val (partitionKeys, clusteringKeys, allFields) = parsePrimaryKey(keyAliases, columnAliases, definitions)

            val attributes = parsed.attrs
            val properties = Utils.readFieldForce(attributes, "properties") as HashMap<String, *>
            val (compaction, compression, caching) = parseComplexProperties(properties)

            val gcGraceSeconds = (properties.getOrDefault("gc_grace_seconds", "-1") as String).toInt()
            val readRepairChance = properties.getOrDefault("read_repair_chance", "").toString()
            val dcLocalReadRepairCahnce = properties.getOrDefault("dclocal_read_repair_chance", "").toString()
            val bloomFilterFpChance = properties.getOrDefault("bloom_filter_fp_chance", "0.01").toString().toDouble()

            return ParsedCreateTable(parsed.table(),
                    allFields,
                    partitionKeys,
                    clusteringKeys,
                    gcGraceSeconds,
                    readRepairChance,
                    dcLocalReadRepairCahnce,
                    bloomFilterFpChance,
                    compaction,
                    compression,
                    caching,
                    "",
                    "")
        }

        private fun parsePrimaryKey(partitionKeyList: List<ColumnIdentifier>,
                                    clustering: List<ColumnIdentifier>,
                                    all: Map<Any, Any>
        ): Triple<List<String>, List<ClusteringKey>, Map<String, Field>> {

            val partitionKeys = mutableListOf<String>()
            val clusteringKeys = mutableListOf<ClusteringKey>()
            val allFields = mutableMapOf<String, Field>()

            for (partitionKey in partitionKeyList) {
                val keyName = Utils.readFieldForce(partitionKey, "text") as String
                partitionKeys.add(keyName)
            }

            for (c in clustering) {
                val keyName = Utils.readFieldForce(c, "text") as String
                val clusteringKey = ClusteringKey(keyName, Direction.ASC)
                clusteringKeys.add(clusteringKey)
            }

            for ((field, cqltype) in all) {
                val name = when (field) {
                    // ColumnIdentifier comes from CREATE TABLE statements
                    is ColumnIdentifier -> Utils.readFieldForce(field, "text") as String
                    // RawSelector comes from CREATE MV statements
                    is RawSelector -> Utils.readFieldForce(field, "selectable").toString()
                    else -> throw RuntimeException("Couldn't parse field $field")
                }
                val dataType = cqltype.toString()
                val newField = Field(dataType)
                allFields[name] = newField
            }

            return Triple(partitionKeys, clusteringKeys, allFields)

        }


        fun noCaching(): Caching {
            return Caching("NONE", "NONE")
        }

        fun parseCachingfromJSON(data: String): Caching {
            val parsed = Utils.parseJson(data)
            val keys = parsed.get("keys")
            val rows = parsed.get("rows_per_partition")

            return Caching(keys.asText(), rows.asText())
        }

        private fun parseCompaction(compactionClass: String, options: Map<String, String>): CompactionDetail {
            val shortName = when (compactionClass.substringAfterLast(".")) {
                "SizeTieredCompactionStrategy" -> "STCS"
                "LeveledCompactionStrategy" -> "LCS"
                "DateTieredCompactionStrategy" -> "DTCS"
                "TimeWindowCompactionStrategy" -> "TWCS"
                "CFSCompactionStrategy" -> "CFS"
                else -> compactionClass
            }
            return CompactionDetail(compactionClass, shortName, options)

        }

        private fun parseComplexProperties(properties: HashMap<String, *>): Triple<CompactionDetail, Compression, Caching> {

            // compaction
            val compactionOptions = properties["compaction"]
            val compaction = if (compactionOptions != null) {
                val compactionClass = (compactionOptions as HashMap<String, String>).remove("class")!!
                parseCompaction(compactionClass, compactionOptions)
            } else {
                CompactionDetail.none()
            }

            // compression
            val compression = if (properties.containsKey("compression") && (properties["compression"] as Map<*, *>).count() > 0) {
                val compressionOptions = properties["compression"] as HashMap<String, String>
                if (compressionOptions.containsKey("enabled") && compressionOptions["enabled"].equals("false")) {
                    emptyCompression()
                } else {
                    val algo = if (compressionOptions.containsKey("sstable_compression")) {
                        compressionOptions.remove("sstable_compression")!!
                    } else {
                        compressionOptions.remove("class")!!
                    }
                    Compression(algo, compressionOptions)
                }
            } else {
                emptyCompression()
            }

            // caching
            val cachingOption = properties["caching"]
            val caching: Caching = if (cachingOption != null) {
                // 2.0 output
                when (cachingOption) {
                    is String -> {
                        when {
                            cachingOption.toString() == "KEYS_ONLY" -> {
                                Caching("ALL", "NONE")
                            }
                            cachingOption.toString() == "ALL" -> {
                                Caching("ALL", "ALL")
                            }
                            cachingOption.startsWith("{") -> {
                                parseCachingfromJSON(cachingOption)
                            }
                            cachingOption.toLowerCase() == "none" -> {
                                Caching("NONE", "NONE")
                            }
                            else -> {
                                throw Exception("Unknown type of table caching: $cachingOption")
                            }
                        }
                    } // anything past 2.0 that I know of: caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                    is HashMap<*, *> -> {
                        Caching(cachingOption["keys"]!!.toString(), cachingOption["rows_per_partition"]!!.toString())
                    }
                    else -> {
                        throw Exception("The caching type is unknown")
                    }
                }
            } else {
                noCaching()
            }

            return Triple(compaction, compression, caching)
        }

    }
}