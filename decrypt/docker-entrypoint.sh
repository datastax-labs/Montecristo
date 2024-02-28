#!/usr/bin/env bash
# set -x
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

ENCRYPTION_KEY="$1"

cd artifacts
ENCRYPTED_FILES=$(find . -iname "*.enc")
NUM_ENCRYPTED_FILES=$(echo "${ENCRYPTED_FILES}" | wc -w)

if [ "${NUM_ENCRYPTED_FILES}" -gt 0 ] && [ -z "${ENCRYPTION_KEY}" ]; then
    echo
    echo "Encrypted files were downloaded, however no encryption key path was provided. Aborting decryption process."
    exit 0
fi

set -e

FILE_COUNT=1
for file in ${ENCRYPTED_FILES}
do
    OUTFILE=${file%.enc}
    echo
    echo "Decrypting $file to $OUTFILE (${FILE_COUNT} / ${NUM_ENCRYPTED_FILES})"
    openssl enc -aes-256-cbc -d -in "$file" -out $OUTFILE -pass pass:"${ENCRYPTION_KEY}" \
        || (echo "Falling back to md5 decryption" && openssl enc -aes-256-cbc -d -md md5 -in "$file" -out $OUTFILE -pass pass:"${ENCRYPTION_KEY}") \
        || echo "Decryption failed for file $file"
    FILE_COUNT=$(( ${FILE_COUNT} + 1 ))
done

echo "Decrypting done"
