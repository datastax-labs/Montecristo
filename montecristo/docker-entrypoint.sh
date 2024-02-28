#!/usr/bin/env bash
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -x

echo "Generating Database"
java -Xms2g -Xmx6G -cp montecristo/target/montecristo.jar com.datastax.discovery.GenerateMetricsDBKt -d /root/basedir/metrics.db -i /root/extracted --overwrite

# com.datastax.montecristo.GenerateMetricsDBKt -d /root/metrics.db -i /root/extracted --overwrite
echo "Running montecristo"
java -Xms2g -Xmx6G -jar montecristo/target/montecristo.jar /root/basedir/
		
mv montecristo-report.txt /root/reports/
