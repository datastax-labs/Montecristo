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

package com.datastax.montecristo.fileLoaders

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.datastax.montecristo.commands.DiscoveryArgs
import com.datastax.montecristo.fileLoaders.parsers.IFileParser
import com.datastax.montecristo.fileLoaders.parsers.application.CassandraYamlParser
import com.datastax.montecristo.fileLoaders.parsers.application.DseYamlParser
import com.datastax.montecristo.fileLoaders.parsers.application.JVMSettingsParser
import com.datastax.montecristo.fileLoaders.parsers.nodetool.GossipInfoParser
import com.datastax.montecristo.fileLoaders.parsers.nodetool.InfoParser
import com.datastax.montecristo.fileLoaders.parsers.nodetool.RingParser
import com.datastax.montecristo.fileLoaders.parsers.nodetool.StatusParser
import com.datastax.montecristo.fileLoaders.parsers.os.*
import com.datastax.montecristo.fileLoaders.parsers.schema.SchemaParser
import com.datastax.montecristo.fileLoaders.parsers.statistics.SSTableStatisticsParser
import com.datastax.montecristo.helpers.FileHelpers
import com.datastax.montecristo.logs.LogFileFinder
import com.datastax.montecristo.logs.LogSettingsParser
import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.metrics.IMetricServer
import com.datastax.montecristo.metrics.SqlLiteMetricServer
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.LoadError
import com.datastax.montecristo.model.Node
import com.datastax.montecristo.model.Workload
import com.datastax.montecristo.model.application.CassandraYaml
import com.datastax.montecristo.model.application.DseYaml
import com.datastax.montecristo.model.application.SolrConfig
import com.datastax.montecristo.model.metrics.BlockedTasks
import com.datastax.montecristo.model.metrics.SSTableStatistics
import com.datastax.montecristo.model.nodetool.*
import com.datastax.montecristo.model.os.*
import com.datastax.montecristo.model.os.DStat.DStat
import com.datastax.montecristo.model.storage.*
import com.datastax.montecristo.model.versions.DatabaseVersion
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.parser.ParserException
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.reflect.KFunction

class Artifacts(
    private val rootDirectory: String,
    val discoveryArgs: DiscoveryArgs? = null) {

    private val logger = LoggerFactory.getLogger(this::class.java)
    private val metricsDbLocation = "$rootDirectory/metrics.db"
    private val artifactsDirectory = "$rootDirectory/extracted"
    private lateinit var metricsDb: IMetricServer
    lateinit var issueID: String

    // moved to a function, out of init so that we can run tests without needing a DB created on instantiation.
    fun discoverIssueID() {
        val issueFile = FileHelpers.getFilesWithName(artifactsDirectory, "issue.txt")
        issueID = if (issueFile.isNotEmpty()) {
            issueFile.first().readText()
        } else {
            "Not Specified"
        }
    }

    // moved to a function, out of init so that we can run tests without needing a DB created on instantiation.
    fun prepareMetricsDB() {
        metricsDb = SqlLiteMetricServer(metricsDbLocation)
    }

    fun createSearcher(cluster: Cluster, daysToInclude : Long) : Searcher {
        return Searcher(FSDirectory.open(File(rootDirectory, "logSearchIndex").toPath()), daysToInclude, cluster.getLogMinDatesToUse(daysToInclude))
    }

    fun createCluster(): Cluster {
        //val metricsDb = sqlLiteMetricServer(metricsDbLocation)
        //metricsDb.prepareAll()
        val loadErrors = mutableListOf<LoadError>()

        // if we're on AWS, the metadata should show it, otherwise it's empty.
        val isAWS: Boolean
        val files = FileHelpers.getFilesWithName(artifactsDirectory, "aws-instance-type.txt")
        // if we can't find an AWS file we just assume it's not
        isAWS = if (files.isEmpty())
            false
        else {
            val content = files.first().readText()
            content.isNotEmpty() && !content.contains("Error 404")
        }


        // Create the nodes with their child objects.
        val nodes: List<Node> = File(artifactsDirectory).listFiles().filter { it.isDirectory }
            .map {
                val node = createNode( File(it.path), loadErrors)
                node
            }.toList()

        // do any nodes report themselves as being dse? if so treat the whole cluster that way
        val isDse = nodes.any { it.isDse }
        // which version is the most common
        val version = determineMostCommonVersion(discoveryArgs, nodes, isDse, loadErrors)

        val blockedTasks = BlockedTasks(metricsDb)

        val schema = SchemaParser.loadSchemaFromFiles(version, metricsDb, rootDirectory, artifactsDirectory)

        // check if we are missing folders - folder count vs 1 node's node tool status metadata count (about all the other nodes as well as itself)
        if (nodes.size != nodes.first().status.nodes.size) {
            loadErrors.add(LoadError("ALL", "The number of node folders does not match the number of nodes within nodetool status. If were not expecting nodes to be missing, this must be investigated before proceeding with the report."))
        }
        if (nodes.any { it.status.hasDownedNodes() }) {
            loadErrors.add(LoadError("ALL", "Nodes are being reported as down within the nodetool status files. If this is not expected, please investigate before proceeding with the report."))
        }

        return Cluster(
                nodes,
                isAWS,
                isDse,
                version,
                schema,
                blockedTasks,
                metricsDb,
                loadErrors
        )
    }


    /**
     * Creates a node from a top level extracted directory
     * use label if you need the name
     * This is future proofing for when a node might not just be a host but a host:port combination
     */
    fun createNode(fp: File, loadErrors: MutableList<LoadError>): Node {

        val hostname = FileHelpers.getHostname(fp)

        if (hostname == "unknown node") {
            loadErrors.add(LoadError(fp.absolutePath, "Unable to get hostname from file or folder name"))
        }


        val osConfiguration = loadOSConfiguration(fp, hostname, loadErrors)
        val storage = loadStorage(fp, hostname, loadErrors)


        val status = loadViaParser(fp, listOf("nodetool/status"), StatusParser, hostname, loadErrors, Status(emptyList(), ""))
        val ring = loadViaParser(fp, listOf("nodetool/ring"), RingParser, hostname, loadErrors, emptyList())
        val tpStats = simpleLoad<TpStats>(fp, listOf("nodetool/tpstats"), hostname, loadErrors)
        val info = loadViaParser(fp, listOf("nodetool/info"), InfoParser, hostname, loadErrors, Info())
        val describeCluster = simpleLoad<DescribeCluster>(fp, listOf("nodetool/describecluster"), hostname, loadErrors)
        val gossipInfoMap = loadViaParser(fp, listOf("nodetool/gossipinfo"), GossipInfoParser, hostname, loadErrors, emptyMap())

        val cassandraYamlFileList = FileHelpers.getFilesWithName(File(fp, "conf").absolutePath, "cassandra.yaml")
        val cassandraYaml = if (cassandraYamlFileList.isNotEmpty()) {
            try {
                CassandraYamlParser.parse(cassandraYamlFileList.first())
            } catch (e: ParserException) {
                loadErrors.add(LoadError(hostname, "Unable to parse cassandra.yaml - it is mot likely malformed which is a serious issue and must be investigated : " + e.toString().replace("\n", " ")))
                CassandraYaml(JsonNodeFactory.instance.objectNode())
            }
        } else {
            loadErrors.add(LoadError(hostname, "No cassandra.yaml file found"))
            CassandraYaml(JsonNodeFactory.instance.objectNode())
        }

        var jvmSettings = JVMSettingsParser.parse(osConfiguration.psAux.getNodeProcessLine())
        if (jvmSettings.gcAlgorithm == GCAlgorithm.UNKNOWN) {
            val settingsFromOptions = getJvmSettingsFromOptions(fp.absolutePath)
            if (settingsFromOptions.isNotEmpty()) {
                loadErrors.add(LoadError(hostname, "GC settings have been estimated from all jvm(.*-server)?.options files combined and MUST be manually validated"))
                jvmSettings = JVMSettingsParser.parse(settingsFromOptions)
            }
            else {
                loadErrors.add(LoadError(hostname, "GC settings cannot be determined or estimated"))
            }
        }
        val logbackSettings = LogFileFinder.getLogbackConfig(fp)
        if (logbackSettings.isEmpty()) {
            loadErrors.add(LoadError(hostname, "No logback.xml file found"))
        }
        val logSettings = LogSettingsParser.parseLoggers(logbackSettings)

        // listen address - this can be more complex, if they set listen_address then we have the IP and we are good to continue
        // if they have set the listen_interface however, we do not know what the IP is, and this is a bit of a pain since so much
        // other data is based on the IP of the node - so it prevents linking them.
        val yamlListenAddress = cassandraYaml.listenAddress
        val listenAddress = if (yamlListenAddress.isNotEmpty() && yamlListenAddress != "null") {
            yamlListenAddress
        } else {
            // grab the interface and look it up on the ifconfig for the address
            val listenInterface = cassandraYaml.getValueFromPath("listen_interface", "")
            osConfiguration.ifConfig.get(listenInterface) ?: "unknown"
        }

        // broadcast address, if set, grab it, otherwise its the same as the listen address
        val yamlBroadcastAddress = cassandraYaml.broadcastAddress
        val broadCastAddress = if (yamlBroadcastAddress.isNotEmpty() && yamlBroadcastAddress != "null") {
            yamlBroadcastAddress
        } else {
            //
            listenAddress
        }

        // now try to find the gossip info for this node, based on listen address, or broadcast, or provide a null since we can't find it.
        // if both addresses are names, meaning we can't get into the right gossip, we can use the host_id of the node.
        val gossipInfo = gossipInfoMap.getOrDefault(listenAddress, gossipInfoMap.getOrDefault(broadCastAddress,  gossipInfoMap.values.firstOrNull{ it.hostId == info.getHostID() }))

        val dseYamlFileList = FileHelpers.getFilesWithName(File(fp, "conf").absolutePath, "dse.yaml")
        val dseYamlExists = dseYamlFileList.isNotEmpty()

        // try to resolve the version of the software
        val versionInfo = getVersionFileContents(fp.absolutePath)
        val isDse = determineIfNodeIsDSE(gossipInfo, dseYamlExists, versionInfo)
        val version = determineVersion(isDse, gossipInfo, versionInfo, hostname, loadErrors)

        val dseYaml = if (dseYamlFileList.isNotEmpty()) {
            try {
                DseYamlParser.parse(dseYamlFileList.first().readText())
            } catch (e: Exception) {
                loadErrors.add(LoadError(hostname, "Unable to parse dse.yaml - it is most likely malformed which and must be investigated : " + e.toString().replace("\n", " ")))
                DseYaml(JsonNodeFactory.instance.objectNode())
            }
        } else {
            // only raise this report warning if we believe the tarball is a DSE tarball
            if (isDse) {
                loadErrors.add(LoadError(hostname, "No dse.yaml file found"))
            }
            DseYaml(JsonNodeFactory.instance.objectNode())
        }

        // we now need to ascertain if search, spark or graph is running - skip if not dse
        val workload: List<Workload> =
                if (isDse) {
                    // the gossip info knows about workload types, if we have nothing assume cassandra only
                    gossipInfo?.getWorkloads() ?: listOf(Workload.CASSANDRA)
                } else {
                    // by default it must have this
                    listOf(Workload.CASSANDRA)
                }

        // SSTable Statistics parsing
        val statsFilesTxt = FileHelpers.getFilesWithName(File(fp, "sstable-statistics").absolutePath, ".*-Statistics\\.txt")
        val statsFilesDB = FileHelpers.getFilesWithName(File(fp, "sstable-statistics").absolutePath, ".*-Statistics\\.db")
        // if we have text files use them, otherwise send in the DB files to be converted
        val ssTableStatistics = when {
            statsFilesTxt.isNotEmpty() -> {
                SSTableStatisticsParser.parse(statsFilesTxt)
            }
            statsFilesDB.isNotEmpty() -> {
                SSTableStatisticsParser.parse(statsFilesDB)
            }
            else -> {
                SSTableStatistics(emptyList())
            }
        }

        // Solr Config Loading
        val solrConfig : MutableMap<String, SolrConfig> = mutableMapOf()
        val solrConfigFiles = FileHelpers.getFilesWithName(File(fp ,"solr").absolutePath, "solrconfig.xml")
        for (solrConfigFile in solrConfigFiles) {
            val solrIndexName = solrConfigFile.parentFile.name
            val dbFactory = DocumentBuilderFactory.newInstance()
            val dBuilder = dbFactory.newDocumentBuilder()
            try {
                val doc = dBuilder.parse (solrConfigFile)
                solrConfig[solrIndexName] = SolrConfig(doc)
            } catch (e: Exception) {
                logger.warn("Invalid solr file")
                loadErrors.add(LoadError(hostname, "Failed to parse solr file : ${solrConfigFile.parentFile.name}"))
            }
        }

        // Check that the yaml cluster name is the same as the describe cluster name - if it is not, then this tarball is unlikely to be valid.
        if (describeCluster.getName() != cassandraYaml.clusterName) {
            loadErrors.add(LoadError(hostname, "Cluster_name value in Describe Cluster and in Cassandra.Yaml do not match. Do not continue without verifying that you have the correct config files."))
        }
        return Node(hostname, osConfiguration, storage, status, ring, tpStats, info, describeCluster, listenAddress, cassandraYaml, dseYaml, jvmSettings, logSettings, workload, gossipInfoMap, isDse, version, ssTableStatistics, solrConfig.toMap())
    }

    private fun loadOSConfiguration(fp : File, hostname : String, loadErrors: MutableList<LoadError>) : Configuration {
        val meminfo = loadViaParser(fp, listOf("os/meminfo", "os-metrics/meminfo"), MemInfoParser, hostname, loadErrors, MemInfo(emptyMap()))
        val sysctl = loadViaParser(fp, listOf("os/sysctl", "os-metrics/sysctl"), SysCtlParser, hostname, loadErrors, Sysctl(emptyMap()))
        val lscpu = loadViaParser(fp, listOf("os/lscpu", "os-metrics/lscpu"), LsCpuParser, hostname, loadErrors, LsCpu(emptyMap<String, String>()))
        val ifConfig = loadViaParser(fp, listOf("os/ifconfig", "os-metrics/ifconfig"), IfConfigParser, hostname, loadErrors, IfConfig(emptyMap<String, String>()))

        val dstat = loadViaParser(fp, listOf("storage/dstat"), DstatParser, hostname, loadErrors, DStat(emptyList()))

        val javaVersionFile = FileHelpers.getFile(fp, listOf("os/java-version", "os-metrics/java-version", "java-version", "java_version"))
        val javaVersion = if (javaVersionFile.exists()) {
            val javaVersionText = javaVersionFile.readText()
            javaVersionText.ifBlank {
                loadErrors.add(LoadError(hostname, "Java version file was empty"))
                "No Java version information was found within the diagnostic files."
            }
        } else {
            loadErrors.add(LoadError(hostname, "No java version file found"))
            ""
        }

        val psAux = simpleLoad<PsAux>(fp, listOf("os/ps-aux", "os-metrics/ps-aux"), hostname, loadErrors)
        val ntp = loadViaParser(fp, listOf("network/ntpq-p", "os/ntpq_p", "os-metrics/ntpq_p"), NtpParser, hostname, loadErrors, Ntp("UNKNOWN_SERVER", null, null, null))
        val env = simpleLoad<Env>(fp, listOf("os/env", "os-metrics/env"), hostname, loadErrors)

        // limits, the limits can be in 2 places, depending on package vs tarball, either limits.conf or limits.d/cassandra.conf
        // load both and merge the text from them together.
        val limits = simpleLoad<Limits>(fp, listOf("os/limits.conf"), hostname, loadErrors)
        val limitsd = simpleLoad<Limits>(fp, listOf("os/limits.d/cassandra.conf"), hostname, loadErrors)
        val totalLimits = Limits(limits.data + limitsd.data,"")

        val hugePages = simpleLoad<TransparentHugePageDefrag>(fp, listOf("os/transparent_hugepage-defrag"), hostname, loadErrors)

        val osInfoFile = FileHelpers.getFile(fp, listOf("os/os", "os.txt"))
        val osInfoLines = if (osInfoFile.exists()) {
            osInfoFile.readLines()
        } else {
            emptyList()
        }
        val osInfoJsonFile = FileHelpers.getFile(fp, listOf("os-info.json"))
        val osInfoJsonLines = if (osInfoJsonFile.exists()) {
            osInfoJsonFile.readLines()
        } else {
            emptyList()
        }

        val osVersion = getOSVersion(osInfoLines, osInfoJsonLines, hostname, loadErrors)
        return Configuration(meminfo, sysctl, lscpu, dstat, javaVersion, psAux, ntp, env, ifConfig, osVersion, totalLimits, hugePages)

    }

    private fun loadStorage(fp : File, hostname : String, loadErrors: MutableList<LoadError>) : Storage {
        val blockdev = simpleLoad<Blockdev>(fp, listOf("os/blockdev-report", "os/blockdev_report", "os-metrics/blockdev-report", "os-metrics/blockdev_report", "blockdev-report","blockdev_report"), hostname, loadErrors)
        val fstab = simpleLoad<Fstab>(fp, listOf("os/fstab", "os-metrics/fstab"), hostname, loadErrors)
        val dfSize = simpleLoad<DfSize>(fp, listOf("storage/df-size", "os-metrics/df", "os/df"), hostname, loadErrors)

        val storageLocations = simpleLoad<StorageLocations>(fp,listOf("os-metrics/disk_device.txt"), hostname, loadErrors )
        val diskDeviceMap = mutableMapOf<String, DiskDevice>()
        val diskDeviceFileList = FileHelpers.getFilesWithName(File(fp, "storage").absolutePath, "read_ahead_kb-.*\\.txt")
        for (diskDevice in diskDeviceFileList) {
            try {
                val deviceName = diskDevice.nameWithoutExtension.split("-")[1]
                val readAhead = diskDevice.readLines().firstOrNull()?.toLongOrNull()
                val schedulerFiles = FileHelpers.getFilesWithName(File(fp, "storage").absolutePath, "scheduler-$deviceName\\.txt")
                if (schedulerFiles.isNotEmpty()) {
                    val scheduler = schedulerFiles.first().readLines().firstOrNull() ?: ""
                    diskDeviceMap[deviceName] = DiskDevice(readAhead, scheduler)
                } else {
                    diskDeviceMap[deviceName] = DiskDevice(readAhead, "")
                    loadErrors.add(LoadError(hostname, "Failed to find disk scheduler file for device : $deviceName"))
                }
            } catch (e: Exception) {
                logger.warn("Invalid read_ahead_kb file")
                loadErrors.add(LoadError(hostname, "Failed to parse disk device file : ${diskDevice.parentFile.name}"))
            }
        }
        val lsBlk = loadViaParser(fp, listOf("os/lsblk_custom.txt"), LsBlkParser, hostname, loadErrors, emptyMap())

        return Storage(storageLocations, diskDeviceMap, blockdev, dfSize, fstab, lsBlk)
    }

    private fun getJvmSettingsFromOptions(searchDirectory: String): String {
        logger.info("Attempting to reconstruct JVM settings from jvm(.*-server)?.options...")
        val jvmFiles = FileHelpers.getFilesWithName(searchDirectory, "jvm(.*-server)?.options")
        if (jvmFiles.isNotEmpty()) {
            val jvmServerFiles = jvmFiles.filter { it.endsWith("-server.options") }

            // XXX flags from different jvm*-server.options files are appended. half of these are not actually used.
            var jvmFlags = if (jvmServerFiles.isNotEmpty()) {
                    jvmServerFiles.flatMap { it.readLines() }.filter { !it.startsWith("#") && it.contains("-X") }.joinToString(" ")
                } else {
                    jvmFiles.flatMap { it.readLines() }.filter { !it.startsWith("#") && it.contains("-X") }.joinToString(" ")
                }

            if (!jvmFlags.contains("-Xmx")) {
                logger.info("Attempting to reconstruct JVM settings from cassandra-env.sh...")
                val envFiles = FileHelpers.getFilesWithName(searchDirectory, "cassandra-env.sh")
                if (envFiles.isNotEmpty()) {

                    jvmFlags += envFiles.first().readLines().filter {
                            it.startsWith("MAX_HEAP_SIZE=")
                        }.joinToString(" ") { " -Xmx${it.replace("MAX_HEAP_SIZE=\"", "").replace("\"", "")}" }

                    jvmFlags += envFiles.first().readLines().filter {
                            it.startsWith("HEAP_NEWSIZE=")
                        }.joinToString(" ") { " -Xmn${it.replace("HEAP_NEWSIZE=\"", "").replace("\"", "")}" }
                }
            }
            return jvmFlags
        } else {
            // no jvm(.*-server)?.options.
            return ""
        }
    }

    private fun getVersionFileContents(nodeFilePath: String): List<String> {
        val versionFileList = FileHelpers.getFilesWithName(nodeFilePath, "version\\.txt|version")
        return if (versionFileList.isEmpty()) {
            emptyList()
        } else {
            versionFileList.first().readLines()
        }
    }

    internal fun determineIfNodeIsDSE(gossipInfo: GossipInfo?, dseYamlExists: Boolean, versionLines: List<String>): Boolean {
        // primary source - check Gossip info.
        if (gossipInfo != null && !gossipInfo.dseOptions.isEmpty) {
            // we have a dse options in the gossip, so it must be dse
            return true
        } else {
            // secondary source, check if dse.yaml exists
            if (dseYamlExists) {
                return true
            } else {
                // tertiary source, check the nodetool version file for a DSE Version line
                // open the file
                if (versionLines.isNotEmpty()) {
                    // look for the DSE version line
                    if (versionLines.any { it.startsWith("DSE version") }) {
                        return true
                    }
                    // look for a 4 part numeric version number, e.g. split based on the decimal point, and a 4 part number has 3 dots, and splits to 4 items
                    // the 3 part number has 2 dots and splits to 3 parts.
                    if (versionLines.firstOrNull { it.startsWith("ReleaseVersion") }?.split(".")?.size?:0 > 3) {
                        return true
                    }
                }
            }
        }
        // every way of proving it is DSE has been unsuccessful, so we can safely say that it is not.
        return false
    }

    internal fun determineVersion(isDse: Boolean, gossipInfo: GossipInfo?, versionFile: List<String>, hostname: String, loadErrors: MutableList<LoadError>): DatabaseVersion {
        // primary source - gossip
        if (gossipInfo != null) {
            // C*, use release version
            if (!isDse && gossipInfo.releaseVersion.isNotBlank()) {
                return DatabaseVersion.fromString (gossipInfo.releaseVersion, isDse)
            }
            // DSE will use the dse options from the x_11_padding map which is also stored in the gossip info
            if (isDse && gossipInfo.dseOptions.has("dse_version")) {
                return DatabaseVersion.fromString (gossipInfo.dseOptions.get("dse_version").toString().replace("\"", ""), isDse)
            }
        }

        // secondary source - version file
        val findString: String = if (isDse) {
            "DSE version:"
        } else {
            "ReleaseVersion:"
        }
        val releaseLine = versionFile.filter { it.startsWith(findString) }
        if (releaseLine.isNotEmpty()) {
            return (DatabaseVersion.fromString (releaseLine.first().substringAfter(":").trim(), isDse))
        }
        loadErrors.add(LoadError(hostname, "Unable to determine the C* / DSE version for this node"))
        return DatabaseVersion.fromString ("-1.0.0", isDse) // Unknown Version
    }

    internal fun determineMostCommonVersion(args: DiscoveryArgs?, nodes: List<Node>, isDse: Boolean,  loadErrors: MutableList<LoadError>): DatabaseVersion {
        return if (args != null && args.version != "") {
            loadErrors.add(LoadError("all", "Cassandra version information was overridden due to --version being set to ${discoveryArgs?.version}"))
             DatabaseVersion.fromString (args.version)
        } else {
            val versionList = nodes.map { it.databaseVersion }
            // we won't stop analysis, but if we have more than 1 version, warn the consultant
            if (versionList.map { it.toString()}.toSet().size > 1) {
                loadErrors.add(LoadError("all", "More than 1 C* / DSE version was found within the cluster."))
            }
            // what is the most common element of the list
            val mostCommonVersion = versionList.groupingBy { it.toString() }.eachCount().maxByOrNull { it.value }!!.key
            DatabaseVersion.fromString(mostCommonVersion, isDse)
        }
    }

    private fun <T> loadViaParser(parentFolder: File, fileNames: List<String>, parser: IFileParser<T>, hostname: String, loadErrors: MutableList<LoadError>, noFileReturn: T): T {

        val dataFile = FileHelpers.getFile(
            parentFolder,
            fileNames.flatMap {  if (it.endsWith(".txt")) listOf(it) else listOf(it, "$it.txt") })

        return if (dataFile.exists()) {
            try {
                parser.parse(dataFile.readLines())
            } catch (e: Exception) {
                loadErrors.add(LoadError(hostname, "Unable to parse ${dataFile.name} - " + e.message))
                noFileReturn
            }
        } else {
            val fileName = fileNames.first().split("/")[1]
            loadErrors.add(LoadError(hostname, "No $fileName file found"))
            noFileReturn
        }
    }

    private inline fun <reified T> simpleLoad(parentFolder: File, fileNames: List<String>, hostname: String, loadErrors: MutableList<LoadError>): T {
        val dataFile = FileHelpers.getFile(parentFolder, fileNames)
        val actualRuntimeClassConstructor: KFunction<T> = T::class.constructors.first()
        return if (dataFile.exists() && dataFile.length() > 0) {
            actualRuntimeClassConstructor.call(dataFile.readLines(), dataFile.absolutePath)
        } else {
            val fileName = fileNames.first().split("/")[1]
            val message = if (dataFile.exists()) {
                "$fileName exists but is empty"
            } else {
                "$fileName not found"
            }
            loadErrors.add(LoadError(hostname, message))
            actualRuntimeClassConstructor.call(emptyList<String>(), "")
        }
    }

    internal fun getOSVersion(osInfoLines : List<String>, osInfoJsonLines : List<String>, hostname: String, loadErrors: MutableList<LoadError>): String {

        return if (osInfoLines.isNotEmpty()) {
            val prettyName = osInfoLines.firstOrNull { it.startsWith("PRETTY_NAME") }
            if (!prettyName.isNullOrBlank()) {
                // grab the second part of the name, and remove enclosing quotes
                prettyName.split("=").getOrNull(1)?.replace("\"", "") ?: "unknown"
            } else {
                loadErrors.add(LoadError(hostname, "Unable to read OS information from os.txt."))
                "unknown"
            }
        } else {
            // try find os-info.json
            if (osInfoJsonLines.isNotEmpty()) {
                val osVersionLine = osInfoJsonLines.firstOrNull { it.trim().startsWith("\"os_version") }
                val subOsLine = osInfoJsonLines.firstOrNull { it.trim().startsWith("\"sub_os") }
                if (!osVersionLine.isNullOrBlank()) {
                    val osVersionValue = osVersionLine.split(":").getOrNull(1)?.replace("\"", "") ?: "unknown"
                    // There may be a sub_os, e.g. CentOS Linux is the sub-os and the os_version is 7.9.2009, if there is a sub_os, show it.
                    if (!subOsLine.isNullOrBlank()) {
                        subOsLine.split(":").getOrNull(1)?.replace("\"", "")?.trim() + " " + osVersionValue.trim()
                    } else {
                        osVersionValue.trim()
                    }
                } else {
                    loadErrors.add(LoadError(hostname, "Unable to read OS information from os-info.json"))
                    "unknown"
                }
            } else {
                loadErrors.add(LoadError(hostname, "Unable to read OS information."))
                "unknown"
            }
        }
    }
}
