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

import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

data class SolrConfig(val baseData: Document) {

    fun getMergeFactor() : Int? {
        return getConfigSettingAsInt("/config/indexConfig/mergeFactor")
    }

    fun getMergeMaxMergeCount() : Int? {
        return getConfigSettingAsInt("/config/indexConfig/mergeScheduler/int[@name='maxMergeCount']")
    }

    fun getMergeMaxThreadCount() : Int? {
        return getConfigSettingAsInt("/config/indexConfig/mergeScheduler/int[@name='maxThreadCount']")
    }

    fun getFacetLimit() : Int? {
        return getConfigSettingAsInt("/config/requestHandler[@name='solr_query']/lst[@name='defaults']/str[@name='facet.limit']")
    }

    fun usesBinaryFieldOutputTransformer() : Boolean {
        val node = getElement("/config/fieldOutputTransformer[@class='com.datastax.bdp.cassandra.index.solr.functional.BinaryFieldOutputTransformer']")
        return node != null
    }

    fun getRealTimeIndexing(): Boolean? {
        return getConfigSetting("/config/indexConfig/rt")?.toBoolean()
    }

    fun getAutoSoftCommitMaxTime() : Int? {
        return getConfigSettingAsInt ("/config/updateHandler[@class='solr.DirectUpdateHandler2']/autoSoftCommit/maxTime")
    }

    fun isFilterCacheSettingOk() : Boolean {
        val element = getElement("/config/query/filterCache")
        return if (element == null) {
            true
        } else {
            element.attributes.getNamedItem("class").nodeValue == "solr.SolrFilterCache"
                    && element.attributes.getNamedItem("highWaterMarkMB").nodeValue == "2048"
                    && element.attributes.getNamedItem("lowWaterMarkMB").nodeValue == "1024"
        }
    }

    fun getMaxBooleanClauses() : Int? {
        return getConfigSettingAsInt ("/config/query/maxBooleanClauses")
    }

    fun getDirectoryFactory() : Pair<String,String>? {
        val df = getElement("/config/directoryFactory")
        return if (df == null) {
            null
        } else {
            Pair(df.attributes.getNamedItem("name").nodeValue, df.attributes.getNamedItem("class").nodeValue)
        }
    }


    private fun getConfigSetting(xpath : String) : String? {
        val xpFactory = XPathFactory.newInstance()
        val xPath = xpFactory.newXPath()
        val settingValue = xPath.evaluate(xpath, baseData, XPathConstants.NODESET) as NodeList
        return if (settingValue.length > 0) {
            settingValue.item(0).textContent
        } else {
            null
        }
    }

    private fun getConfigSettingAsInt(xpath : String) : Int? {
        return getConfigSetting(xpath)?.toIntOrNull()
    }

    private fun getElement(xpath : String) : Node? {
        val xpFactory = XPathFactory.newInstance()
        val xPath = xpFactory.newXPath()
        val potentialNode = xPath.evaluate(xpath, baseData, XPathConstants.NODE)
        return if (potentialNode != null) {
            potentialNode as Node
        } else {
            null
        }
    }
}
