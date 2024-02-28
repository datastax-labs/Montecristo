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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.datastax.montecristo.helpers.FileHelpers
import com.datastax.montecristo.helpers.Utils
import com.datastax.montecristo.logs.logger
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.model.schema.*
import com.datastax.montecristo.model.versions.DatabaseVersion
import org.apache.cassandra.cql3.CQL3Type
import org.apache.cassandra.cql3.FieldIdentifier
import org.apache.cassandra.cql3.UTName
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement
import org.apache.cassandra.cql3.statements.schema.KeyspaceAttributes
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.commons.lang3.reflect.MethodUtils
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Method

object SchemaParser {

    // file access pushed outside of the schema creation to make testing easier and invert the dependency
    fun loadSchemaFromFiles(
        databaseVersion: DatabaseVersion,
        metricsDb: IMetricServer,
        rootDirectory: String,
        artifactsDirectory: String
    ): Schema {
        // zero length schema files are considered equivalent to the schema file not existing, so this doesn't let empty files
        // bypass the schema checks.
        val schemasPerNode =
            FileHelpers.getFilesWithName(artifactsDirectory, "schema\\.cql|schema").toList().filter { it.length() > 0 && !it.isDirectory }
        val fullSchemasPerNode =
            FileHelpers.getFilesWithName(artifactsDirectory, "full-schema").toList().filter { it.length() > 0 && !it.isDirectory }
        val schemasAtRoot = FileHelpers.getFile(File(rootDirectory), listOf("schema.cql", "schema"))
        return createSchema(databaseVersion, metricsDb, schemasPerNode, fullSchemasPerNode, schemasAtRoot)
    }

    fun createSchema(
        databaseVersion: DatabaseVersion,
        metricsServer: IMetricServer,
        schemasPerNode: List<File>,
        fullSchemasPerNode: List<File>,
        schemasAtRoot: File
    ): Schema {

        // Order is that we will use the schema per node if it exists (and it is consistent)
        // If it doesn't we will use the full-schema file (and it is consistent - although if the schema file isn't the full-schema won't be,
        //      but this covers when there is no schema.cql)
        // if neither of those exist, or things disagree we drop to the root schema file
        // if no root then we look for the schema versions and choose the most agreed (MONT-177)
        // if there is no version information, or the agreed version is missing in the folders, we throw the exception,
        // since that that point we need manual intervention to figure out whats the work around.

        logger.info("schema files : " + schemasPerNode.size.toString())
        logger.info("full schema files : " + fullSchemasPerNode.size.toString())
        // check if all schemas are the same
        val schemaPerNodeHashes = schemasPerNode
            .map { schemaFile -> schemaFile.readText() }
            .map { schemaContent -> Pair(schemaContent, schemaContent.hashCode()) }

        val fullSchemaPerNodeHashes = fullSchemasPerNode
            .map { schemaFile -> schemaFile.readText() }
            .map { schemaContent -> schemaContent.hashCode() }
            .toSet()

        var isSchemaInAgreement = true // assume true until we find out otherwise
        val schema = when {
            schemaPerNodeHashes.toSet().size == 1 -> {
                logger.info("schema files agree - using first node schema")
                schemasPerNode.first().readText()
            }
            fullSchemaPerNodeHashes.size == 1 -> {
                logger.info("full schema files agree - using first node schema")
                fullSchemasPerNode.first().readText()
            }
            schemasAtRoot.exists() -> {
                // we might only have a root schema, in which case we have agreement by default (as far as we know)
                if (schemaPerNodeHashes.toSet().isNotEmpty()) {
                    logger.info("Using root schema - node schemas do not agree")
                    isSchemaInAgreement = false
                } else {
                    logger.info("Using root schema")
                }
                schemasAtRoot.readText()
            }
            else -> {
                logger.info("Schema disagreement, calculating the most agreed.")
                isSchemaInAgreement = false
                // there is no set of schema files in agreement, and no root file
                // MONT-323 - Original a schema disagreement uses the schema version hash from the JMX loaded into the database to resolve.
                // However, when we have no JMX this fails and the report halt execution. We only needed the JMX to get the schema hash for display later, but that can use
                // GossipInfo instead to source that information now - so we don't actually need the hash, or even which node, we just need to use the most common text.

                // find the schema hash code which is the most common - this is returning a map of the hash (key) and the count (value)
                val mostAgreedVersionSchemaHash =
                    schemaPerNodeHashes.groupingBy { it.second }.eachCount().maxByOrNull { it.value }?.key
                // use this to grab the schema content from one of the nodes with this value
                schemaPerNodeHashes.filter { it.second == mostAgreedVersionSchemaHash }.toList().first().first
            }
        }
        // we either have a file, or we will of thrown the exception
        return parse(schema, metricsServer, databaseVersion, isSchemaInAgreement)
    }

    fun parse(
        cql: String,
        metricsServer: IMetricServer,
        databaseVersion: DatabaseVersion,
        isSchemaInAgreement: Boolean,
    ): Schema {
        val rawStatements = getRawStatements(cql)

        val keyspaces = getKeyspaces(rawStatements)
        val objects = getObjects(rawStatements, metricsServer, databaseVersion)
        val tablesRaw = getTables(objects)
        // process tables to handle the MVs having incorrect field values
        // it will copy them from the base table to create a new table object for the list
        val tables = tablesRaw.map {
            if (it.isMV) {
                processMVTable(tablesRaw, it)
            } else {
                it
            }
        }

        val indexes = getIndexes(objects)
        val saiIndexes = getSaiIndexes(objects)
        val sasiIndexes = getSasiIndexes(objects)
        val searchIndexes = getSearchIndexes(objects)
        val customTypes = getTypes(rawStatements)
        val customFunctions = getFunctions(rawStatements)
        return Schema(
            keyspaces,
            tables,
            indexes,
            saiIndexes,
            sasiIndexes,
            searchIndexes,
            customTypes,
            customFunctions,
            isSchemaInAgreement
        )
    }


    private fun processMVTable(listOfTables: List<Table>, t: Table): Table {
        // when dealing with an MV, we need to return some aspects which come from the base table, not the MV
        // primarily the field list and types of those fields, since the MV does not know them
        val replacementFieldList = if (t.fields.isEmpty()) {
            // MV is a select *, we need to copy them all
            listOfTables.firstOrNull { it.name == t.baseTableName }?.fields
        } else {
            // we have a partial field list, we need to create a new one based on filtering the base table fields
            // on the mv field names
            val fieldList = listOfTables.firstOrNull { it.name == t.baseTableName }?.fields
            fieldList?.filter { t.fields.keys.contains(it.key) }
        } ?: emptyMap()

        return Table(t.metricsDb,
                t.databaseVersion,
                t.partitionKeys,
                t.name,
                replacementFieldList,
                t.clusteringColumns,
                t.compactionStrategy,
                t.gcGrace,
                t.readRepair,
                t.dcLocalReadRepair,
                t.compression,
                t.caching,
                t.fpChance,
                t.isMV,
                t.is2i,
                t.isSai,
                t.isSasi,
                t.isSearchIndex,
                t.autoLoadFromMetricsDB,
                t.originalCQL,
                t.indexName,
                "")
    }

    fun getKeyspaces(rawStatements: List<String>): List<Keyspace> {
        val keyspaces = rawStatements.filter { it.toLowerCase().startsWith("create keyspace") }
        return keyspaces.map {

            val parsed = Utils.parseCQL(it)
            var name = ""
            var strategy = ""
            val options: MutableList<Pair<String, Int>> = mutableListOf()
            if (parsed != null) {
                // we have to do this because the name and fields are declared private
                val f: Field = FieldUtils.getField(parsed.javaClass, "keyspaceName", true)
                name = FieldUtils.readField(f, parsed, true) as String
                val f2: Field = FieldUtils.getField(parsed.javaClass, "attrs", true)
                val attributes = FieldUtils.readField(f2, parsed, true) as KeyspaceAttributes

                val m1: Method? = MethodUtils.getMatchingMethod(attributes.javaClass, "getAllReplicationOptions")
                m1?.isAccessible= true
                val keyspaceOptions = m1?.invoke(attributes) as Map<String, String>
                for (item in keyspaceOptions) {
                    // have to filter out the replication strategy class, its a string-> string, and not wanted for the RF options collection
                    if (item.key != "class") {
                        options.add(Pair(item.key, item.value.toInt()))
                    }
                }
                val m2: Method? = MethodUtils.getMatchingMethod(attributes.javaClass, "getReplicationStrategyClass")
                m2?.isAccessible = true
                strategy = m2?.invoke(attributes) as String

                Keyspace(name, options, strategy)
            }
            Keyspace(name, options, strategy)
        }
    }

    fun getTables(tables: List<Table>): List<Table> {
        return tables.filter { !it.is2i && !it.isSai && !it.isSasi && !it.isSearchIndex }
    }

    fun getIndexes(tables: List<Table>): List<Table> {
        return tables.filter { it.is2i }
    }

    private fun getSaiIndexes(tables: List<Table>): List<Table> {
        return tables.filter { it.isSai }
    }

    private fun getSasiIndexes(tables: List<Table>): List<Table> {
        return tables.filter { it.isSasi }
    }

    private fun getSearchIndexes(tables: List<Table>): List<Table> {
        return tables.filter { it.isSearchIndex }
    }


    private fun getObjects(rawStatements: List<String>, metricsServer: IMetricServer, databaseVersion: DatabaseVersion): List<Table> {

        val statements = rawStatements
            .filter {
                (it.contains("CREATE TABLE") && !it.contains("Warning")) ||
                        it.contains("CREATE MATERIALIZED VIEW") ||
                        it.contains("CREATE INDEX") ||
                        it.contains("CREATE CUSTOM INDEX") ||
                        it.contains("^USE.*".toRegex())
            }

        val tables = mutableListOf<Table>()
//            val tables = statements.map { Table.fromCqlString(myController, it) }
        val useStatement = "^USE\\s+(.*)".toRegex()
        var currentKeyspace = ""

        statements.forEach {
            // if it's a use statement we need to get the keyspace name
            val matches = useStatement.find(it)
            if (matches != null) {
                currentKeyspace = matches.groupValues[1]
            } else {
                var createStatement = it
                // check if the CREATE operation is at the start of the string, if not find where it starts and
                // use the substring to create the table
                val createStatementIndex = it.indexOf("CREATE")

                if (createStatementIndex > 0) {
                    createStatement = it.substring(createStatementIndex)
                }
                val table = fromCqlString(createStatement, currentKeyspace, metricsServer, databaseVersion)
                table.onSuccess { table ->
                    tables.add(table)
                }
            }

        }
        return tables
    }

    private fun getTypes(rawStatements: List<String>): List<UserDefinedType> {
        val types = rawStatements.filter { it.toLowerCase().startsWith("create type") }
        val parsedTypes = types.map { Pair(Utils.parseCQL(it), it) }.toList()

        return parsedTypes.map {
            when (it.first) {
                is CreateTypeStatement.Raw -> fromCreateTypeStatement(it.first as CreateTypeStatement.Raw, it.second)
                else -> throw Exception("Not a type")
            }
        }
    }

    private fun fromCreateTypeStatement(parsed: CreateTypeStatement.Raw, rawStatement: String): UserDefinedType {

        val name = Utils.readFieldForce(parsed, "name") as UTName?
        val columnNames = Utils.readFieldForce(parsed, "fieldNames") as List<FieldIdentifier>
        val columnTypes = Utils.readFieldForce(parsed, "rawFieldTypes") as List<CQL3Type.Raw>

        val fields = columnNames.associate { Pair(it.toString(), columnTypes[columnNames.indexOf(it)].toString()) }

        return UserDefinedType(
            name?.keyspace + "." + name?.stringTypeName,
            fields,
            rawStatement
        )
    }

    private fun getFunctions(rawStatements: List<String>): Map<String,String> {
        val functionStatements = rawStatements.filter {
            it.toLowerCase().startsWith("create function") || it.toLowerCase().startsWith("create or replace function")
        }
        val lowerCaseFunctions = functionStatements.map { it.toLowerCase() }
        // need to parse out the name, and put the rest into the map as the function definition
        val findFunctionNameRegex = Regex("create [or replace ]?function ([\\w|.]*)((.|\\n)*)")

        val functionList =
            functionStatements.map { it.toLowerCase() }.associate { fn ->
                if (findFunctionNameRegex.matches(fn)) {
                    val regexFnLine = findFunctionNameRegex.find(fn)
                    Pair(regexFnLine!!.groupValues[1], fn)
                } else {
                    Pair("Unable to parse function name", fn)
                }
            }
        return functionList
    }


    fun fromCqlString(
        cql: String,
        useKeyspace: String = "",
        metricsDb: IMetricServer,
        databaseVersion: DatabaseVersion
    ): Result<Table> {
        lateinit var parsed: ParsedCreateTable
        try {
            parsed = ParsedCreateTable.fromCQL(cql)
        } catch (ex: Exception) {
            // the most anticipated problem is that there is a DSE specific type on the table, which means the table can't be analyzed
            // currently
            // TODO : Figure out parsing DSE schema
            logger.error("CQL statement could not be parsed - starts with : " + cql.substring(0, 30))
            return Result.failure(ex)
        }

        // split the name
        val tmp = "CREATE (TABLE|MATERIALIZED VIEW|INDEX|CUSTOM INDEX)\\s([^\\s]+).*[\\(|AS]".toRegex().find(cql)
        if (tmp == null) {
            logger.warn("Could not parse CQL: $cql")
            return Result.failure(Exception())
        }
        val nameGroups = tmp.groupValues

        val keyspaceAndTable = nameGroups[2].trim()

        val isMV = nameGroups[1] == "MATERIALIZED VIEW"
        val is2i = nameGroups[1] == "INDEX"
        // need to handle search vs SAI
        val isSai = nameGroups[1] == "CUSTOM INDEX" && cql.toLowerCase().contains("using 'storageattachedindex'")
        val isSasi = nameGroups[1] == "CUSTOM INDEX" && cql.toLowerCase()
            .contains("using 'org.apache.cassandra.index.sasi.sasiindex'")
        val isSearchIndex = nameGroups[1] == "CUSTOM INDEX" && cql.toLowerCase()
            .contains("using 'com.datastax.bdp.search.solr.cql3solrsecondaryindex'")

        var baseTable: String = parsed.baseTableName
        // we have both things we need
        val name = if (keyspaceAndTable.contains(".")) {
            keyspaceAndTable
        } else {
            if (isSai || isSasi || isSearchIndex) {
                // the parsed.name is the base table, and the keyspaceAndTable is the index name (no keyspace. is in the name)
                baseTable = parsed.name
                parsed.name.split(".")[0] + ".$keyspaceAndTable"
            } else {
                if (!is2i) {
                    println("must be 2.0 - Using $useKeyspace in absence of an actual keyspace.")
                    "$useKeyspace.$keyspaceAndTable"
                } else {
                    parsed.name
                }
            }
        }

        val caching = parsed.caching


        return Result.success(
            Table(
                metricsDb, databaseVersion,
                parsed.partitionKeys,
                name,
                parsed.fields,
                parsed.clusteringKeys,
                parsed.compaction,
                parsed.gcGraceSeconds,
                parsed.readRepairChance,
                parsed.dclocalReadRepairChance,
                parsed.compression,
                caching = caching,
                fpChance = parsed.bloomFilterFPChance,
                isMV = isMV,
                is2i = is2i,
                isSai = isSai,
                isSasi = isSasi,
                isSearchIndex = isSearchIndex,
                originalCQL = cql,
                indexName = parsed.indexName,
                baseTableName = baseTable
            )
        )
    }

    fun getCompression(cql: String): Compression {
        val compression = Regex("compression\\s*=\\s*(\\{[^}]+\\})")
            .find(cql)?.groups?.get(1)?.value ?: "UNKNOWN_COMPRESSION"
        val mapper = ObjectMapper()

        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)

        val data: java.util.HashMap<*, *>? = mapper.readValue(compression, kotlin.collections.HashMap::class.java)
        val cls = data!!.getOrDefault("class", "NONE") as String
        @Suppress("UNCHECKED_CAST")
        return Compression(cls.replace("org.apache.cassandra.io.compress.", ""), data as HashMap<String, String>)
    }

    private fun getRawStatements(cql: String): List<String> {
        return cql
            .replace('\n', ' ')
            .replace("(\\/\\*.*?\\*\\/)".toRegex(), "")
            .split(";")
            .map { it.trim() }
    }
}
