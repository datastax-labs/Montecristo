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

package com.datastax.montecristo.commands

import com.datastax.montecristo.fileLoaders.Artifacts
import com.datastax.montecristo.fileLoaders.parsers.ExecutionConfigParser
import com.datastax.montecristo.helpers.stripHTMLComments
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.astra.Astra
import com.datastax.montecristo.sections.astra.Operations
import com.datastax.montecristo.sections.configuration.*
import com.datastax.montecristo.sections.datamodel.*
import com.datastax.montecristo.sections.infrastructure.*
import com.datastax.montecristo.sections.operations.*
import com.datastax.montecristo.sections.security.Authentication
import com.datastax.montecristo.sections.security.NetworkEncryption
import com.datastax.montecristo.sections.structure.*
import com.datastax.montecristo.utils.OutputSectionCounter
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

class GenerateDiscoveryReport(private var rootDirectory: String, private var discoveryArgs: DiscoveryArgs) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    // we have to use lateinit because the directory doesn't exist yet

    private var discoveryDocDirectory: String = "$rootDirectory/reports/montecristo"

    private val indexDocument: File = File("$discoveryDocDirectory/content/_index.md")

    lateinit var artifacts: Artifacts
    lateinit var cluster : Cluster
    lateinit var executionProfile: ExecutionProfile
    private lateinit var logSearch : Searcher
    /**
     * Filename of the summary page.  We ned to write it out blank initially, then rewrite it later after we have
     * processed all the items.
     *
     */
    lateinit var summary: String


    /**
     * Each DocumentSection can return a MutableList<Recommendation>.  They get added to the bottom of each page automatically
     * in #writeMarkdownDocument() and then are saved in this map to be listed in the summary page.
     */
    private var summaryRecommendations = mutableMapOf<RecommendationPriority, MutableList<Recommendation>>().withDefault { mutableListOf() }

    init {
        summaryRecommendations[RecommendationPriority.IMMEDIATE] = mutableListOf()
        summaryRecommendations[RecommendationPriority.NEAR] = mutableListOf()
        summaryRecommendations[RecommendationPriority.LONG] = mutableListOf()
    }

    fun execute() {
        // this is very important... otherwise we could delete from pwd or the root.
        if(rootDirectory.trim() == "") {
            throw Exception("Root directory can't be empty")
        }

        if(!discoveryArgs.dryRun)
            initReportDir(discoveryDocDirectory)

        // Everything is now derived in the myController class
        artifacts = Artifacts(rootDirectory, discoveryArgs = discoveryArgs)
        // discover the issued ID
        artifacts.discoverIssueID()
        // prepare the Metrics DB
        artifacts.prepareMetricsDB()
        // create the data model
        cluster = artifacts.createCluster()
        // Load the execution configuration
        executionProfile = ExecutionConfigParser.parse(File(discoveryArgs.executionProfilePath), cluster.loadErrors)
        // create the log searcher
        logSearch = artifacts.createSearcher(cluster, executionProfile.limits.numberOfLogDays)

        // All top level headings have their own pages, and should not be included as a part of
        // another section, re-ordering is a lot harder when section headings are included with a
        // sub-section.

        // Intro for the analysis
        writeIntro()
        // Infrastructure
        writeInfrastructure()
        // Configuration
        writeConfiguration()
        // Security
        writeSecurity()
        // Schema review
        writeDataModel()
        // Log / JMX analysis
        if(System.getenv("SKIP_LOGS") != "yes") {
            writeOperations()
        }
        // Astra guardrails and operations
        writeAstra()

        // rewrite the summary now that we have everything!!!!!!!!!
        if(!discoveryArgs.dryRun)
            File(summary).writeText(
                Summary(summaryRecommendations.getValue(RecommendationPriority.IMMEDIATE),
                                    summaryRecommendations.getValue(RecommendationPriority.NEAR),
                                    summaryRecommendations.getValue(RecommendationPriority.LONG)).getDocument(
                    cluster,
                    logSearch,
                    mutableListOf(),
                    executionProfile
                ))

        // write final markdown document
        if(!discoveryArgs.dryRun) {
            val gdocExporter = GenerateSingleDocument()
            gdocExporter.dir = File(rootDirectory, "reports/montecristo").toString()
            gdocExporter.generateSingleDocument()
        } else {
            logger.info("Not writing final document (dry run flag enabled!)")
        }

        displayRecommendationsInConsole()
        logger.info("Analysis terminated")
    }

    private fun writeIntro() {
        // intro items
        // we save the summary so we can rewrite it later
        // we don't need to pass valid params here, this is just a temp file
        summary = writeMarkdownDocument("summary.md", Summary(listOf(), listOf(), listOf()))
        writeMarkdownDocument("introduction.md", CollectedArtifacts())
    }

    private fun writeInfrastructure() {
        writeMarkdownDocument("infrastructure_heading.md", SectionHeadingDocument("Infrastructure Overview"))
        writeMarkdownDocument("infrastructure_overview.md", InfrastructureOverview())
        writeMarkdownDocument("infrastructure_path.md", Path())
        writeMarkdownDocument("infrastructure_storage.md", Storage())
        writeMarkdownDocument("infrastructure_swap.md", Swap())
        writeMarkdownDocument("infrastructure_ntp.md", Ntp())
        writeMarkdownDocument("infrastructure_java_version.md", JavaVersion())
        writeMarkdownDocument("infrastructure_os_config_overview.md", OSConfigOverview())
    }

    private fun writeConfiguration() {
        val configHeading = if (cluster.isDse) { "DSE Configuration" } else { "Cassandra Configuration" }
        writeMarkdownDocument("configuration_heading.md", SectionHeadingDocument(configHeading))
        writeMarkdownDocument("configuration_cassandra_version.md", Version())
        writeMarkdownDocument("configuration_custom_settings.md", CustomSettings())
        writeMarkdownDocument("configuration_configuration_mismatches.md", ConfigurationMismatches())
        writeMarkdownDocument("configuration_logging.md", Logging())
        writeMarkdownDocument("configuration_paths.md", Paths())
        writeMarkdownDocument("configuration_seeds.md", Seeds())
        writeMarkdownDocument("configuration_address_bindings.md", AddressBindings())
        writeMarkdownDocument("configuration_compaction.md", CompactionSettings())
        writeMarkdownDocument("configuration_streaming.md", StreamingSettings())
        writeMarkdownDocument("configuration_node_communications.md", NodeCommunications())
        writeMarkdownDocument("configuration_snitch.md", Snitch())
        writeMarkdownDocument("configuration_disk_policy.md", DiskFailurePolicy())
        writeMarkdownDocument("configuration_trickle_fsync.md", TrickleFsync())
        writeMarkdownDocument("configuration_thread_pools.md", ThreadPools())
        writeMarkdownDocument("configuration_memtable_storage.md", MemtableStorageSettings())
        writeMarkdownDocument("configuration_jvm.md", JavaHeapConfiguration())
        writeMarkdownDocument("configuration_ring.md", Ring())
    }

    private fun writeSecurity() {
        writeMarkdownDocument("security_heading.md", SectionHeadingDocument("Security Configuration"))
        writeMarkdownDocument("security_network_encryption.md", NetworkEncryption())
        writeMarkdownDocument("security_authentication.md", Authentication())
    }

    private fun writeDataModel() {
        writeMarkdownDocument("data_model_heading.md", SectionHeadingDocument("Data Model"))
        // start with basic settings and stuff we can give recommendations on with big analysis
        writeMarkdownDocument("data_model_schema_agreement.md", SchemaVersions())
        writeMarkdownDocument("data_model_ops_center.md", OpsCenter())
        writeMarkdownDocument("data_model_replication.md", ReplicationStrategy())
        writeMarkdownDocument("data_model_secondary_indexes.md", SecondaryIndexes())
        writeMarkdownDocument("data_model_unused_tables.md", UnusedTables())
        writeMarkdownDocument("data_model_gc_grace_seconds.md", GcGraceSeconds())
        writeMarkdownDocument("data_model_custom_types.md", CustomTypes())
        writeMarkdownDocument("data_model_collections.md", CollectionTypes())
        writeMarkdownDocument("data_model_readrepair.md", ReadRepair())
        writeMarkdownDocument("data_model_bloom_filters.md", BloomFilters())
        // I want this before the compaction and compression sections because we need to know if we're dealing with MVs
        // MVs do have their own compaction settings that can be altered
        writeMarkdownDocument("data_model_materialized_views.md", MaterializedViews())
        // the big 3 performance ones
        writeMarkdownDocument("data_model_compaction.md", Compaction())
        writeMarkdownDocument("data_model_compression.md", Compression())
        writeMarkdownDocument("data_model_top_tables.md", TableMetadataTopTables())
        writeMarkdownDocument("data_model_caches.md", Caches())
        writeMarkdownDocument("data_model_sstable_count.md", SSTableCounts())
        writeMarkdownDocument("data_model_data_distribution.md", TablePartitionBalance())
    }

    private fun writeOperations() {
        writeMarkdownDocument("operations__logs_heading.md", SectionHeadingDocument("Cassandra Logs"))
        writeMarkdownDocument("operations_log_analysis_durations.md", LogAnalysisDuration())
        writeMarkdownDocument("operations_blocked_flush.md", BlockedFlushWriters())
        writeMarkdownDocument("operations_dropped_hints.md", DroppedHints())
        writeMarkdownDocument("operations_hinted_handoff_tasks.md", HintedHandoffTasks())
        writeMarkdownDocument("operations_dropped_messages.md", DroppedMessages())
        writeMarkdownDocument("operations_jvm_gc_stats.md", JvmGcStats())
        writeMarkdownDocument("operations_batches.md", Batches())
        writeMarkdownDocument("operations_failed_repair.md", RepairSessions())
        writeMarkdownDocument("operations_commit_log_sync.md", CommitLogSync())
        writeMarkdownDocument("operations_gossip_pause.md", GossipLogPausesWarnings())
        writeMarkdownDocument("operations_compacting_large_partitions.md", CompactingLargePartitions())
        writeMarkdownDocument("operations_aborted_hints.md", AbortedHints())
        // TODO : these two sections need to be put together
        writeMarkdownDocument("operations_tombstones_per_read.md", TombstonesPerRead())
        writeMarkdownDocument("operations_tombstone_warnings.md", TombstoneWarnings())
        writeMarkdownDocument("operations_aggregation_queries.md", AggregationQueries())
        writeMarkdownDocument("operations_prepared_statements.md", PreparedStatements())
    }

    private fun writeAstra() {
        writeMarkdownDocument("astra_heading.md", SectionHeadingDocument("Astra"))
        writeMarkdownDocument("astra_guardrails.md", Astra())
        writeMarkdownDocument("astra_operations.md", Operations())
    }

    private fun createHugoDirs() {
        if(discoveryArgs.dryRun) {
            // don't do any of this fun stuff if we're in a dry run
            return
        }

        File(discoveryDocDirectory, "content").walkTopDown().drop(1).forEach { it.deleteRecursively() }
//        File(discoveryDocDirectory, "content").walkTopDown().drop(1).forEach { println("Deleting $it") }

        File(discoveryDocDirectory).mkdirs()

        val inputStream = this::class.java.getResourceAsStream("/hugo.zip")
        val zis = ZipInputStream(inputStream)
        var entry: ZipEntry?
        entry = zis.nextEntry
        do {
            val fileName = entry!!.name
            if (entry.isDirectory) {
                //
                File(discoveryDocDirectory, fileName).mkdirs()
                logger.info("Created dir $discoveryDocDirectory/$fileName")
            } else {
                val newFile = File("$discoveryDocDirectory/", fileName)
                val fos = FileOutputStream(newFile)
                val buffer = ByteArray(1024)
                var length = zis.read(buffer)
                while (length != -1) {
                    fos.write(buffer, 0, length)
                    length = zis.read(buffer)
                }

                fos.close()
                logger.info("Created $discoveryDocDirectory/$fileName")
            }
        } while (run {
                entry = zis.nextEntry
                entry
            } != null)

        // now we can open the index, which will just be links to each hugo document
        File(discoveryDocDirectory, "content").mkdir()
        indexDocument.delete()
        indexDocument.createNewFile()
        indexDocument.appendText("""+++
            |title = "DataStax Discovery Document Contents"
            |+++
            |
            |
            """.trimMargin()
        )
        indexDocument.appendText("<br/>\n\n## **[Draft Discovery Report](exporter)**<br/><br/>")
        indexDocument.appendText("<br/>\n## **[Astra Report](astra)**")
    }


    private fun initReportDir(discoveryDocDirectory: String) {
        if(discoveryArgs.dryRun) {
            return
        }
        File("$discoveryDocDirectory/content").mkdirs()
        createHugoDirs()
    }

    private fun writeMarkdownDocument(documentName: String, documentContent: DocumentSection) : String {

        // mutable recommendations list gets populated in the documentContent
        // we do this because we want to use the document section interface
        // and you can't define a property on an interface
        val recommendations = mutableListOf<Recommendation>()

        var content = try {
            documentContent.getDocument(cluster, logSearch, recommendations, executionProfile).trim()
        } catch (e: Exception) {
            cluster.loadErrors.add(LoadError("All","Error generating $documentName $e"))
            logger.error("Error generating $documentName ${e.stackTraceToString()}")
            ""
        }

        if(recommendations.isNotEmpty()) {
            logger.info("Found ${recommendations.size} recommendations for $documentName.")
            // is the last line of content a ---?  if so, we don't need a leading --- with our recommendations
            if (content.lines().last().trim() != "---") {
                content += "\n\n---"
            }
            content += "\n\n"
            for (r in recommendations) {
                val cleanedUp = formatRecommendation(r)
                content += "_**Recommendation**: ${cleanedUp}_\n\n---\n\n"
                // add the recommendation to the central collection, with the recommendation type that we know this one falls under
                summaryRecommendations[r.priority]!!.add(r)
            }

        } else {
            // always end with a newline
            content += "\n"
            logger.info("No recommendations for $documentName")

        }

        // return before we write out the recommendations
        if(discoveryArgs.dryRun)
            return ""

        // if there is no content, do not write it - this allows a section to choose to be skipped
        if (content.isEmpty())
            return ""

        val sequenceNo = "%03d".format(OutputSectionCounter.instance.incrementAndGet())
        val fileName = "$discoveryDocDirectory/content/${sequenceNo}_$documentName"

        File(fileName).writeText(content)

        val docNoMd = documentName.replace(".md", "")
        val simpleName = "${sequenceNo}_$docNoMd"

        // write the filename to the document index
        return fileName

    }

    private fun formatRecommendation(r: Recommendation): String {
        // remove HTML comments
        // escape the underscores - renderer can get confused between an italic markup and a genuine underscore
        // the exception is code blocks, underscores in a code block are understood and should not be escaped. (code block starts / ends with a back tick ` )
        val splitLongForm = r.longForm.stripHTMLComments().split("`")
        return if (splitLongForm.size == 1) {
            // simple scenario, no code blocks causing problems
            splitLongForm[0].replace("_", "\\_") // safe to escape the underscores
        } else {
            // complex scenario, we have code blocks to handle
            // array starts at 0 - the 0,2,4,6 would have to be items outside of a code block
            // 1,3,5,7 etc would be the code blocks.
            // we can put the string back together after processing each part.
            val processedStringParts = splitLongForm.mapIndexed { idx: Int, value: String ->
                if (idx % 2 == 1) {
                    "`$value`" // put the value back into code markup, do not escape the underscores
                } else {
                    value // final entry has the ending italic, do not escape it
                }
            }
            processedStringParts.joinToString("")
        }
    }

    private fun displayRecommendationsInConsole() {
        logger.info("  ")
        logger.info("=======================")
        logger.info(">>> Immediate Issues:")
        summaryRecommendations[RecommendationPriority.IMMEDIATE]?.forEach {
            logger.info("* ${it.shortForm}")
        }
        logger.info("  ")
        logger.info("=======================")
        logger.info(">>> Near Term Issues:")
        summaryRecommendations[RecommendationPriority.NEAR]?.forEach {
            logger.info("* ${it.shortForm}")
        }
        logger.info("  ")
        logger.info("=======================")
        logger.info(">>> Long Term Issues:")
        summaryRecommendations[RecommendationPriority.LONG]?.forEach {
            logger.info("* ${it.shortForm}")
        }
    }
}