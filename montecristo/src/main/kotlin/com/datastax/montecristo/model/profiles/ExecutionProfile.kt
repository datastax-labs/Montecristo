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

package com.datastax.montecristo.model.profiles


data class Limits(val numberOfLogDays : Long = 90,
                    val aggregationWarnings: Int = 1000000,
                    val batchSizeWarnings: Int = 1000000,
                    val droppedHints: Int = 100000,
                    val droppedMessages: Int = 1000000,
                    val droppedMessagesPerHourThreshold: Int = 25,
                    val gossipPauseWarnings: Int = 1000000,
                    val gossipPauseTimePercentageThreshold: Double = 5.0,
                    val hintedHandoffMessages: Int = 1000000,
                    val hintedHandoffPerHourThreshold: Int = 25,
                    val preparedStatementWarnings: Int = 1000000,
                    val preparedStatementMessagesPerHourThreshold: Int = 1,
                    val repairErrorMessages: Int = 10000,
                    val repairErrorMessagesDisplayedInReport: Int = 14,
                    val tombstoneWarningsPerDayThreshold: Int = 100,
                    val tokenOwnershipPercentageImbalanceThreshold: Double = 0.2
)

data class ExecutionProfile(val limits : Limits) {

    companion object {
        fun default(): ExecutionProfile {
            return ExecutionProfile(Limits())
        }
    }
}


