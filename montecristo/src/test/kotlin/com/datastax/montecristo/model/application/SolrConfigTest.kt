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

package com.datastax.montecristo.model.application

import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory

class SolrConfigTest {

    lateinit var solrConfig: SolrConfig

    @Before
    fun setUp() {
        val solrConfigFile = File(this.javaClass.getResource("/fileLoaders/parsers/application/solrConfig.xml").path)

        val solrIndexName = "test"
        val dbFactory = DocumentBuilderFactory.newInstance()
        val dBuilder = dbFactory.newDocumentBuilder()
        val doc = dBuilder.parse (solrConfigFile)
        solrConfig = SolrConfig(doc)
    }

    @Test
    fun testMergeFactor() {
        assertThat(solrConfig.getMergeFactor()).isEqualTo(13)
    }
    @Test
    fun testMaxMergeThreadCount() {
        assertThat(solrConfig.getMergeMaxThreadCount()).isEqualTo(3)
    }
    @Test
    fun testMaxMergeMergeCount() {
        assertThat(solrConfig.getMergeMaxMergeCount()).isEqualTo(6)
    }
    @Test
    fun testIsFilterCacheSettingOk() {
        assertThat(solrConfig.getMergeMaxMergeCount()).isEqualTo(6)
    }
    @Test
    fun testFacetLimit() {
        assertThat(solrConfig.getFacetLimit()).isEqualTo(-1)
    }

    @Test
    fun testUsesBinaryFieldOutputTransformer() {
        assertThat(solrConfig.usesBinaryFieldOutputTransformer()).isEqualTo(false)
    }

    @Test
    fun testGetRealTimeIndexingSetting() {
        assertThat(solrConfig.getRealTimeIndexing()).isEqualTo(false)
    }

    @Test
    fun testGetAutoSoftCommit() {
        assertThat(solrConfig.getAutoSoftCommitMaxTime()).isEqualTo(10000)
    }

    @Test
    fun isFilterCacheSettingOk() {
        assertThat(solrConfig.isFilterCacheSettingOk()).isEqualTo(false)
    }

    @Test
    fun testMaxBooleanClauses() {
        assertThat(solrConfig.getMaxBooleanClauses()).isEqualTo(1024)
    }

    @Test
    fun testGetDirectoryFactory() {
        val df = solrConfig.getDirectoryFactory()
        assertThat(df).isNotNull
        assertThat(df!!.first).isEqualTo("DirectoryFactory")
        assertThat(df!!.second).isEqualTo("solr.StandardDirectoryFactory")
    }



}
