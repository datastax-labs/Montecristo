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

package com.datastax.montecristo.model.os

import com.datastax.montecristo.model.os.DStat.DStat

/**
 * Wrapper class grouping up for all types of node-level OS configuration information
 */
data class Configuration(val memInfo: MemInfo,
                         val sysctl: Sysctl,
                         val lsCpu: LsCpu,
                         val dstat: DStat,
                         val javaVersion: String,
                         val psAux: PsAux,
                         val ntp: Ntp,
                         val env: Env,
                         val ifConfig: IfConfig,
                         val osReleaseName : String,
                         val limits : Limits,
                         val hugePages : TransparentHugePageDefrag)