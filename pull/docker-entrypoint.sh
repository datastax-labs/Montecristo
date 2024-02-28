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

# replace placeholders with envars
sed -ri "s|AWS_ACCESS_KEY_ID|${AWS_ACCESS_KEY_ID}|" ${WORKDIR}/.aws/credentials
sed -ri "s|AWS_SECRET_ACCESS_KEY|${AWS_SECRET_ACCESS_KEY}|" ${WORKDIR}/.aws/credentials
set -x

case "$1" in
    # print usage
    ""      )   echo Please provide a ticket name, in the form of PROJ-####, or 'bash'.
                exit;;
esac
case "$2" in
    # print usage
    ""      )   echo Please provide a bucket name.
                exit;;
esac

# sync s3 bucket to mounted volume
aws s3 sync s3://${2}/${1} artifacts/
