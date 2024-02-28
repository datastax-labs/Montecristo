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

package com.datastax.montecristo.model.astra

abstract class ILimits {
    abstract var maxFieldsPerTable : Int
    abstract var maxTablesPerDatabase : Int
    abstract var maxFieldsPerUdt : Int
    abstract var maxSecondaryIndexesPerTable : Int
    abstract var maxMaterializedViewsPerTable : Int
    abstract var maxSaiPerTable : Int
    abstract var maxSaiPerDatabase : Int
    abstract var pageSizeFailureThresholdInKb : Long
    abstract var inSelectCartesianProductFailureThreshold : Long
    abstract var partitionKeysInSelectFailureThreshold : Long
    abstract var tombstoneWarnThreshold : Long
    abstract var tombstoneFailureThreshold : Long
    abstract var batchSizeWarnThreshold : Long
    abstract var batchSizeFailureThreshold : Long
    abstract var unloggedBatchAcrossPartitionsWarnThreshold : Long
    abstract val columnValueSizeFailureThreshold : Long
}