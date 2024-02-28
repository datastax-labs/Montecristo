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

package com.datastax.montecristo.sections.astra

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.Workload
import com.datastax.montecristo.model.application.ConfigValue
import com.datastax.montecristo.model.astra.AstraDSE
import com.datastax.montecristo.model.astra.Classic
import com.datastax.montecristo.model.astra.ILimits
import com.datastax.montecristo.model.astra.Serverless
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.utils.MarkdownTable

class Astra : DocumentSection {

    // Limits taken from : https://docs.datastax.com/en/astra/docs/datastax-astra-database-limits.html

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)
        try {
            val astraClassicGuardrails = processGuardrails(cluster, Classic())
            val astraServerlessGuardrails = processGuardrails(cluster, Serverless())
            val astraDSEDBGuardrails = processGuardrails(cluster, AstraDSE())
            val commonGuardrails = processCommonGuardrails(cluster)

            args["astraCommon"] = commonGuardrails.toString()
            args["astraClassic"] = astraClassicGuardrails.toString()
            args["astraServerless"] = astraServerlessGuardrails.toString()
            args["astraDSE"] = astraDSEDBGuardrails.toString()
        } catch (e: Exception) {
            cluster.loadErrors.add(LoadError("All","Error generating astra/astra_guardrails.md $e"))
        }
        return compileAndExecute("astra/astra_guardrails.md", args)
    }

    private fun processGuardrails(cluster: Cluster, limits : ILimits) : MarkdownTable {
        val astraGuardrails = MarkdownTable("Check", "Threshold", "Result", "Notes")

        // Number of columns per table
        val fieldListRow = astraGuardrails.addRow().addField("Fields Per Table").addField(limits.maxFieldsPerTable.toString())
        val listOfTablesExceedingFieldLimit = cluster.schema.getUserTables().filter { it.fields.size > limits.maxFieldsPerTable }
        if (listOfTablesExceedingFieldLimit.isNotEmpty()) {
            val tableDetails =
                listOfTablesExceedingFieldLimit.joinToString("<br />") { "${it.name} : ${it.fields.size} " }
            fieldListRow.addField("Fail").addField("${listOfTablesExceedingFieldLimit.size} table(s) exceeded the threshold.<br /> $tableDetails")
        } else {
            fieldListRow.addField("Pass").addField("")
        }

        // Tables - number of tables within the database
        val exceedsMaxTableRow = astraGuardrails.addRow().addField("Tables per Database").addField(limits.maxTablesPerDatabase.toString())
        val exceedsTableLimit = cluster.schema.getUserTables().size > limits.maxTablesPerDatabase
        if (exceedsTableLimit) {
            exceedsMaxTableRow.addField("Fail").addField("${cluster.schema.getUserTables().size} tables are within the database.")
        } else {
            exceedsMaxTableRow.addField("Pass").addField("")
        }

        val exceedsUDFTableRow = astraGuardrails.addRow().addField("Fields per UDT").addField(limits.maxFieldsPerUdt.toString())
        val exceedsUDTFieldLimit = cluster.schema.types.filter { it.fields.size > limits.maxFieldsPerUdt }
        if (exceedsUDTFieldLimit.isNotEmpty()) {
            val udtDetails = exceedsUDTFieldLimit.joinToString("<br />") { "${it.name} : ${it.fields.size} " }
            exceedsUDFTableRow.addField("Fail").addField("${exceedsUDTFieldLimit.size} UDTs are defined with more than the number of fields limit of ${limits.maxFieldsPerUdt}. <br /> $udtDetails")
        } else {
            exceedsUDFTableRow.addField("Pass").addField("")
        }

        // Secondary index
        val secondaryIndexPerTableRow = astraGuardrails.addRow().addField("2i Indexes per Table").addField(limits.maxSecondaryIndexesPerTable.toString())
        val exceedsSecondaryIndexLimitTableList = cluster.schema.getUserIndexes().groupingBy { it.getKsAndTable().first + "." + it.getKsAndTable().second }.eachCount().filter { it.value > limits.maxSecondaryIndexesPerTable }
        if (exceedsSecondaryIndexLimitTableList.isNotEmpty()) {
            val secondaryIndexDetails = exceedsSecondaryIndexLimitTableList.map { "${it.key} : ${it.value} " }.joinToString("<br />")
            secondaryIndexPerTableRow.addField("Fail").addField("${exceedsSecondaryIndexLimitTableList.size} table(s) have more than ${limits.maxSecondaryIndexesPerTable} secondary indexes. <br /> $secondaryIndexDetails")
        } else {
            secondaryIndexPerTableRow.addField("Pass").addField("")
        }

        // Materialized view
        val exceedsMvPerTableRowClassic = astraGuardrails.addRow().addField("MVs per Table").addField(limits.maxMaterializedViewsPerTable.toString())
        val exceedsMVLimitPerTableListClassic = cluster.schema.getUserMaterializedViews().groupingBy { it.baseTableName }.eachCount().filter { it.value > limits.maxMaterializedViewsPerTable }
        if (exceedsMVLimitPerTableListClassic.isNotEmpty()) {
            val mvDetails = exceedsMVLimitPerTableListClassic.map { "${it.key} : ${it.value}" }.joinToString("<br />")
            exceedsMvPerTableRowClassic.addField("Fail").addField("${exceedsSecondaryIndexLimitTableList.size} tables have more than ${limits.maxMaterializedViewsPerTable} MVs based on them. <br/> $mvDetails")
        } else {
            exceedsMvPerTableRowClassic.addField("Pass").addField("")
        }

        val udfRow = astraGuardrails.addRow().addField("User Defined Functions").addField(0)
        val udfCount = cluster.schema.getUserFunctions().size
        if (udfCount > 0) {
            val udfDetails = cluster.schema.getUserFunctions().map { it.key }.joinToString("<br />")
            udfRow.addField("Fail").addField("$udfCount UDFs are defined within the database. <br/> $udfDetails")
        } else {
            udfRow.addField("Pass").addField("")
        }

        // SAI Table Failure threshold
        val saiPerTableRowClassic = astraGuardrails.addRow().addField("SAIs per Table").addField(limits.maxSaiPerTable.toString())
        val baseTablesExceedingSAICountClassic = cluster.schema.getUserSAIIndexes().groupingBy { it.baseTableName }.eachCount().filter { it.value > limits.maxSaiPerTable }
        if (baseTablesExceedingSAICountClassic.isNotEmpty()) {
            val saiDetails = baseTablesExceedingSAICountClassic.map { "${it.key} : ${it.value}" }.joinToString("<br />")
            saiPerTableRowClassic.addField("Fail").addField("${baseTablesExceedingSAICountClassic.size} tables have move than ${limits.maxSaiPerTable} SAIs based on them. <br/> $saiDetails")
        } else {
            saiPerTableRowClassic.addField("Pass").addField("")
        }

        // Total number of SAIs
        val saiTotalCountRowClassic = astraGuardrails.addRow().addField("SAIs per Database").addField(
            limits.maxSaiPerDatabase.toString()
        )
        val saiTotalCountClassic = cluster.schema.getUserSAIIndexes().size
        if (saiTotalCountClassic > limits.maxSaiPerDatabase) {
            val saiDetails = cluster.schema.getUserSAIIndexes().joinToString("<br />") { it.name }
            saiTotalCountRowClassic.addField("Fail").addField("The database has $saiTotalCountClassic SAIs which is more than the permitted amount of ${limits.maxSaiPerDatabase}. <br /> $saiDetails")
        } else {
            saiTotalCountRowClassic.addField("Pass").addField("")
        }

        if (limits !is AstraDSE) {
            val solrIndexTableRow = astraGuardrails.addRow().addField("Solr Indexes").addField("0")
            if (cluster.schema.searchIndexes.isNotEmpty()) {
                val searchIndexDetails = cluster.schema.searchIndexes.joinToString("<br />") { it.name }
                solrIndexTableRow.addField("Fail")
                    .addField("${cluster.schema.searchIndexes.size} SolrIndexes exist within the database.<br /> $searchIndexDetails")
            } else {
                solrIndexTableRow.addField("Pass").addField("")
            }
        } else {
            if (cluster.nodes.any { it.workload.contains(Workload.SEARCH) }) {
                checkSolrGuardrails(cluster, astraGuardrails)
            } else {
                astraGuardrails.addRow().addField("Solr Indexes").addField("").addField("Pass").addField("There were no Solr Indexes in the database to analyze")
            }
        }
        /*   Cluster Health
                    Local read latency (ms)
                    Local write latency (ms)
            */

        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Page Size",
            mapOf(Pair("guardrails.page_size_failure_threshold_in_kb", "")),
            limits.pageSizeFailureThresholdInKb
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Cartesian Select",
            mapOf(Pair("guardrails.in_select_cartesian_product_failure_threshold", "")),
            limits.inSelectCartesianProductFailureThreshold
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Multi Partition Selection",
            mapOf(Pair("guardrails.partition_keys_in_select_failure_threshold", "")),
            limits.partitionKeysInSelectFailureThreshold
        )

        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Tombstone Warning Threshold",
            mapOf(Pair("tombstone_warn_threshold", "1000"), Pair("guardrails.tombstone_warn_threshold", "")),
            limits.tombstoneWarnThreshold
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Tombstone Failure Threshold",
            mapOf(Pair("tombstone_failure_threshold", "100000"), Pair("guardrails.tombstone_failure_threshold", "")),
            limits.tombstoneFailureThreshold
        )

        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Batch Size Warning",
            mapOf(
                Pair("batch_size_warn_threshold_in_kb", "64"),
                Pair("guardrails.batch_size_warn_threshold_in_kb", "")
            ),
            limits.batchSizeWarnThreshold
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Batch Size Failure",
            mapOf(
                Pair("batch_size_fail_threshold_in_kb", "640"),
                Pair("guardrails.batch_size_fail_threshold_in_kb", "")
            ),
            limits.batchSizeFailureThreshold
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Multi-Partition Unlogged Batches",
            mapOf(Pair("guardrails.unlogged_batch_across_partitions_warn_threshold", "")),
            limits.unloggedBatchAcrossPartitionsWarnThreshold
        )
        checkConfigSetting(
            cluster,
            astraGuardrails,
            "Maximum Column Size",
            mapOf(Pair("guardrails.column_value_size_failure_threshold_in_kb", "")),
            limits.columnValueSizeFailureThreshold
        )
        return astraGuardrails
    }

    private fun processCommonGuardrails(cluster: Cluster) : MarkdownTable {

        val astraCommonGuardrails = MarkdownTable("Check", "Threshold", "Result", "Notes")

        // Thrift - If on C* 3.11 or DSE < 6.0, then Thrift can / will be enabled unless start_rpc = false, its version dependent
        val startRpc = cluster.getSetting("start_rpc", ConfigSource.CASS, "")
        val thriftServiceRow = astraCommonGuardrails.addRow().addField("Thrift Enabled").addField("false")

        val thriftEnabled = if (cluster.databaseVersion.supportsThrift()) {
            // If this is DSE (and below 6.0) then the default is that it is enabled unless specified as false.
            startRpc.getDistinctValues().any { it.getConfigValue() == "true" || (cluster.isDse && !it.isSet)}
        } else {
            false
        }
        if (thriftEnabled) {
            thriftServiceRow.addField("Fail").addField("The thrift service is configured to be started")
        } else {
            thriftServiceRow.addField("Pass").addField("No thrift service detected.")
        }

        // Not strictly an Astra rule - but it should be
        val sasIndexTableRow = astraCommonGuardrails.addRow().addField("SAS Indexes").addField("0")
        if (cluster.schema.sasiIndexes.isNotEmpty()) {
            val sasIndexDetails = cluster.schema.sasiIndexes.joinToString("<br />") { it.name }
            sasIndexTableRow.addField("Fail").addField("${cluster.schema.sasiIndexes.size} SAS Indexes exist within the database.<br /> $sasIndexDetails")
        } else {
            sasIndexTableRow.addField("Pass").addField("")
        }

        val userTimestampsRow = astraCommonGuardrails.addRow().addField("User Timestamps Enabled").addField("true")
        val userTimestampsEnabled = cluster.getSetting("guardrails.user_timestamps_enabled", ConfigSource.CASS, "")
        if (userTimestampsEnabled.areAllUnset()) {
            userTimestampsRow.addField("Unknown").addField("The `guardrails.user_timestamps_enabled` is not set.")
        } else {
            if (userTimestampsEnabled.values.any { it.value.getConfigValue() == "false" }) {
                userTimestampsRow.addField("Fail").addField("The `guardrails.user_timestamps_enabled` has been set to `false`.")
            } else {
                userTimestampsRow.addField("Pass").addField("")
            }
        }

        val readBeforeWriteRow = astraCommonGuardrails.addRow().addField("Read Before Write List Operations Enabled").addField("false")
        val readBeforeWriteListOperations =
            cluster.getSetting("guardrails.read_before_write_list_operations_enabled", ConfigSource.CASS, "")
        if (readBeforeWriteListOperations.areAllUnset()) {
            readBeforeWriteRow.addField("Unknown").addField("The `guardrails.read_before_write_list_operations_enabled` is not set.")
        } else {
            if (readBeforeWriteListOperations.values.any { it.value.getConfigValue() == "true" }) {
                readBeforeWriteRow.addField("Fail").addField("The `guardrails.read_before_write_list_operations_enabled` has been set to `true`.")
            } else {
                readBeforeWriteRow.addField("Pass").addField("")
            }
        }
        return astraCommonGuardrails
    }

    // Some settings can exist in more than 1 location, tombstones for example, has its own threshold in cassandra.yaml as well as the newer guardrail setting
    // so we need to check both locations.
    private fun checkConfigSetting(cluster: Cluster, mdTable: MarkdownTable, settingName : String , yamlSettings : Map<String,String>, maxValue: Long) {

        // when dealing with multiple settings, per node, as long as something is set, the other non-set's can be ignored, e.g. Tombstone warnings
        // as long as one of the config values for that older / newer is there, its in effect, and the lower of the 2 wins.

        // per setting we can have a different default, a blank default would mean the setting has none. We want to build up the list of settings
        val collatedNodeToSettingMap = mutableMapOf<String, Pair<String, ConfigValue>>()

        yamlSettings.forEach { (settingName, defaultValue) ->
            val nodeToSettingMap = cluster.getSetting(settingName, ConfigSource.CASS, defaultValue).values
            nodeToSettingMap.forEach {
                // if we have something to compare against, and what we are trying to load is not a blank
                if (collatedNodeToSettingMap.containsKey(it.key)) {
                    if (it.value.getConfigValue().isNotBlank()) {
                        // the existing value might be blank (set or not)
                        if (collatedNodeToSettingMap[it.key]!!.second.getConfigValue().isBlank()) {
                            // straight overwrite scenario, no comparison needed
                            collatedNodeToSettingMap[it.key] = Pair(settingName, it.value)
                        } else if (collatedNodeToSettingMap[it.key]!!.second.getConfigValue().toLong() > it.value.getConfigValue().toLong()) {
                            // overwrite with the new lower value set
                            collatedNodeToSettingMap[it.key] = Pair(settingName, it.value)
                        }
                    } // else do nothing, we have no new value to use, leave the existing value where it is.
                } else {
                    collatedNodeToSettingMap[it.key] = Pair(settingName, it.value)
                }
            }
        }
        val finalSettingsDiscovered = collatedNodeToSettingMap.map { it.value }.toSet()
        processConfigSetting(mdTable, finalSettingsDiscovered, settingName, maxValue)
    }

    private fun processConfigSetting(mdTable: MarkdownTable, configSettingValue : Set<Pair<String,ConfigValue>>, settingName : String, maxValue: Long) {
        val md = mdTable.addRow()
        md.addField(settingName).addField(maxValue.toString())
        // There are two key scenarios
        // There is no setting and no default that can be used
        // or
        // we have settings / a default which means we can make a comparison

        if (configSettingValue.all { !it.second.isSet && it.second.defaultValue.isBlank() }) {
            // the default has no meaning, we can not test the guardrail
            md.addField("Unknown").addField("The `${configSettingValue.first().first}` has not been configured.")
        } else {
            val settingName = configSettingValue.first().first
            val usingDefaultOnly = (configSettingValue.all { !it.second.isSet })

            val finalMessage = if (usingDefaultOnly) {
                if (configSettingValue.size == 1) {
                    "has not been configured, so a default value "
                } else {
                    "has not been configured, the largest default value "
                }
            } else {
                if (configSettingValue.size == 1) {
                    "has been set to"
                } else {
                    "has multiple values, the largest of which has been set to"
                }
            }

            // we have a suitable default we can test against - since none of them were set, use the highest default found in the cluster
            val maxSettingValue = if (usingDefaultOnly) {
                configSettingValue.first().second.defaultValue.toLong()
            } else {
                configSettingValue.maxByOrNull { it.second.value.toLong() }!!.second.value.toLong()
            }
            if (maxSettingValue > maxValue) {
                md.addField("Possible Failure")
                    .addField("The `$settingName` $finalMessage $maxSettingValue is being used, which is above the Astra threshold.")
            } else {
                md.addField("Pass")
                    .addField("The `$settingName` $finalMessage $maxSettingValue is being used, which is within the Astra threshold.")
            }
        }
    }

    private fun checkSolrGuardrails(cluster: Cluster, mdTable: MarkdownTable) {
        val facetsRow = mdTable.addRow().addField("Solr - Facet Limits").addField("")
        val breakingFacetLimitSet = cluster.nodes.map {
            it.solrConfig.filter { sc ->
                val facetLimt = sc.value.getFacetLimit() ?: 100
                facetLimt > 20000 || facetLimt == -1
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (breakingFacetLimitSet.isEmpty()) {
            facetsRow.addField("Pass").addField("All search indexes are using facet limits lower than 20k.")
        } else {
            val facetBreakDetails = breakingFacetLimitSet.joinToString("<br />")
            facetsRow.addField("Fail")
                .addField("${breakingFacetLimitSet.size} search indexes are using facet limits greater than 20k.<br /> $facetBreakDetails")
        }

        val mergeFactorRow = mdTable.addRow().addField("Solr - Merge Factor").addField("")
        val mergeFactorSet = cluster.nodes.map {
            it.solrConfig.filter { sc ->
                val mergeFactor = sc.value.getMergeFactor()
                mergeFactor == null|| mergeFactor != 10
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (mergeFactorSet.isEmpty()) {
            mergeFactorRow.addField("Pass").addField("All search indexes are using a merge factor of 10.")
        } else {
            val mergeFactorDetails = mergeFactorSet.joinToString("<br />")
            mergeFactorRow.addField("Fail")
                .addField("${mergeFactorSet.size} search indexes are using merge factors other than 10.<br /> $mergeFactorDetails")
        }

        val mergeFactorMaxMergeCountRow = mdTable.addRow().addField("Solr - Merge Factor, Max Merge Count").addField("")
        val mergeFactorMaxMergeCountSet = cluster.nodes.map {
            it.solrConfig.filter { sc -> sc.value.getMergeMaxMergeCount() != null }.map { sc -> sc.key }
        }.flatten().toSet()

        if (mergeFactorMaxMergeCountSet.isEmpty()) {
            mergeFactorMaxMergeCountRow.addField("Pass").addField("All search indexes have no max merge count set.")
        } else {
            val mergeFactorMaxMergeCountDetails = mergeFactorMaxMergeCountSet.joinToString("<br />")
            mergeFactorMaxMergeCountRow.addField("Fail")
                .addField("${mergeFactorMaxMergeCountSet.size} search indexes are specifying a max merge count.<br /> $mergeFactorMaxMergeCountDetails")
        }

        val mergeFactorMaxThreadCountRow = mdTable.addRow().addField("Solr - Merge Factor, Max Thread Count").addField("")
        val mergeFactorMaxThreadCountSet = cluster.nodes.map {
            it.solrConfig.filter { sc -> sc.value.getMergeMaxMergeCount() != null }.map { sc -> sc.key }
        }.flatten().toSet()

        if (mergeFactorMaxThreadCountSet.isEmpty()) {
            mergeFactorMaxThreadCountRow.addField("Pass").addField("All search indexes have no max thread count set.")
        } else {
            val mergeFactorMaxThreadCountDetails = mergeFactorMaxThreadCountSet.joinToString("<br />")
            mergeFactorMaxThreadCountRow.addField("Fail")
                .addField("${mergeFactorMaxThreadCountSet.size} search indexes are specifying a max thread count.<br /> $mergeFactorMaxThreadCountDetails")
        }


        val fieldTransformersRow = mdTable.addRow().addField("Solr - Field Transformers").addField("")
        val fieldTransformersSet = cluster.nodes.map {
            it.solrConfig.filter { sc -> sc.value.usesBinaryFieldOutputTransformer()
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (breakingFacetLimitSet.isEmpty()) {
            fieldTransformersRow.addField("Pass").addField("All search indexes are not using a Binary Field Output Transformer.")
        } else {
            val fieldTransformersDetails = fieldTransformersSet.joinToString("<br />")
            fieldTransformersRow.addField("Fail")
                .addField("${fieldTransformersSet.size} search indexes are BinaryFieldOutputTransformer as a field transformer.<br /> $fieldTransformersDetails")
        }

        val realTimeRow = mdTable.addRow().addField("Solr - Real Time Indexing").addField("")
        val realTimeSet = cluster.nodes.map {
            it.solrConfig.filter { sc -> sc.value.getRealTimeIndexing()?: false
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (realTimeSet.isEmpty()) {
            realTimeRow.addField("Pass").addField("All search indexes are not using real time indexing.")
        } else {
            val realTimeDetails = realTimeSet.joinToString("<br />")
            realTimeRow.addField("Fail")
                .addField("${fieldTransformersSet.size} search indexes are using real time indexing.<br /> $realTimeDetails")
        }

        val softCommitRow = mdTable.addRow().addField("Solr - Soft Commit").addField("")
        val softCommitSet = cluster.nodes.map {
            it.solrConfig.filter { sc ->
                val softCommit = sc.value.getAutoSoftCommitMaxTime()
                softCommit == null|| softCommit < 10000
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (softCommitSet.isEmpty()) {
            softCommitRow.addField("Pass").addField("All search indexes are using a soft commit of 10 seconds.")
        } else {
            val softCommitDetails = softCommitSet.joinToString("<br />")
            softCommitRow.addField("Fail")
                .addField("${softCommitSet.size} search indexes are using soft commit time other than 10 seconds.<br /> $softCommitDetails")
        }


        val filterCacheRow = mdTable.addRow().addField("Solr - Filter Cache").addField("")
        val filterCacheSet = cluster.nodes.map {
            it.solrConfig.filter { sc -> !sc.value.isFilterCacheSettingOk() }.map { sc -> sc.key }
        }.flatten().toSet()

        if (filterCacheSet.isEmpty()) {
            filterCacheRow.addField("Pass").addField("All search indexes are using default filter cache settings.")
        } else {
            val filterCacheDetails = filterCacheSet.joinToString("<br />")
            filterCacheRow.addField("Fail")
                .addField("${filterCacheSet.size} search indexes are using non-default filter cache settings.<br /> $filterCacheDetails")
        }


        val maxBooleanClauseRow = mdTable.addRow().addField("Solr - Max Boolean Clauses").addField("")
        val maxBooleanClauseSet = cluster.nodes.map {
            it.solrConfig.filter { sc ->
                val maxBooleanClause = sc.value.getMaxBooleanClauses()
                maxBooleanClause == null || maxBooleanClause != 1024
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (maxBooleanClauseSet.isEmpty()) {
            maxBooleanClauseRow.addField("Pass").addField("All search indexes are using a max boolean clause value of 1024.")
        } else {
            val maxBooleanClauseDetails = maxBooleanClauseSet.joinToString("<br />")
            maxBooleanClauseRow.addField("Fail")
                .addField("${maxBooleanClauseSet.size} search indexes are using a max boolean clauses other than 1024.<br /> $maxBooleanClauseDetails")
        }

        val directoryFactoryRow = mdTable.addRow().addField("Solr - Directory Factory").addField("")
        val directoryFactorySet = cluster.nodes.map {
            it.solrConfig.filter { sc ->
                val df = sc.value.getDirectoryFactory()
                df == null || df.first != "DirectoryFactory" || df.second != "solr.StandardDirectoryFactory"
            }.map { sc -> sc.key }
        }.flatten().toSet()

        if (directoryFactorySet.isEmpty()) {
            directoryFactoryRow.addField("Pass").addField("All search indexes are using the standard directory factory.")
        } else {
            val directoryFactoryDetails = directoryFactorySet.joinToString("<br />")
            directoryFactoryRow.addField("Fail")
                .addField("${directoryFactorySet.size} search indexes are using an alternative directory factory.<br /> $directoryFactoryDetails")
        }
    }
}
