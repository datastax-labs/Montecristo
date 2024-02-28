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

class OutputSectionCounter private constructor() {
	  private var index: Int = 0

    private object Holder { val INSTANCE = OutputSectionCounter() }

    companion object {
        val instance: OutputSectionCounter by lazy { Holder.INSTANCE }
    }
    
    fun incrementAndGet(): Int {
      this.index++
      return this.index
	  }
}
