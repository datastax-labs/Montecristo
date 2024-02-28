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

package com.datastax.montecristo.metrics.jmxMetrics

import java.sql.PreparedStatement

class NoMatch : JMXMetric {
    override fun getPrepared(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun bindAll(preparedStatement: PreparedStatement) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun toString() : String {
        return "NoMatch metrics not implemented"
    }
}