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

package com.datastax.montecristo.utils

import com.datastax.montecristo.helpers.Utils

class HumanCount(val num: Long) : Comparable<HumanCount> {
    override fun compareTo(other: HumanCount): Int {
        return this.num.compareTo(other.num)
    }

    override fun toString(): String {
        return Utils.humanReadableCount(num)
    }

    override fun equals(other: Any?): Boolean {
        if(other !is HumanCount)
            return false

        return this.num == other.num
    }

    override fun hashCode(): Int {
        return num.hashCode()
    }
}