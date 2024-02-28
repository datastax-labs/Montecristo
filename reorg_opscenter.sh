#!/bin/bash
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

#
# Start main script execution
#

export BASE_DIR=$1

# all the dirs
echo "Using BASE_DIR directory of ${BASE_DIR}"

if [ -d "$BASE_DIR/extracted/nodes" ];
then
    mv $BASE_DIR/extracted/nodes/* $BASE_DIR/extracted
    rm -Rf $BASE_DIR/extracted/nodes
fi

for f in $BASE_DIR/extracted/*; do
    if [ ! -f "$f/jmx_dump.json" ];
    then
        touch $f/metrics.jmx
    else
        sed -i'.bak' -e 's/NaN/0.0/g' $f/jmx_dump.json
    fi
    # Move conf files
    mv $f/conf/cassandra/* $f/conf/
    mv $f/conf/dse/* $f/conf/
    
    # Move logs
    mv $f/logs/cassandra/* $f/logs
    
    (
        echo "Searching directory $f/logs"
        cd "$f/logs"

        for zip_file in *.zip; do
            echo "Extracting ${zip_file}"
            unzip -o "${zip_file}"
            rm -f "${zip_file}"
        done

        if [ "$(find . -iname "*.gz" | wc -l)" -gt 0 ]; then
            gz_files_processed=1
            for gz_file in *.gz; do
                echo "Extracting ${gz_file}"
                gzip -f -d "${gz_file}"
            done
        fi
    )
    

    # Move schema file
    mv $f/driver/schema $f/schema.cql
done

for f in $BASE_DIR/extracted/*/nodetool/*; do
    # Rename nodetool output files
    filename=${f##*/}; 
    newname="$(dirname $f)/$(echo $filename|cut -d'.' -f1).txt"
    mv $f $newname
done

for f in $BASE_DIR/extracted/*; do
    mv $f/os-metrics $f/os
    mkdir -p $f/os/
    hostname=$(echo ${f##*/}); 
    echo $hostname > $f/os/hostname.txt
done

for f in $BASE_DIR/extracted/*; do 
    if [[ ! -f "$f/os/meminfo" ]];
    then
        totalmem=$(($(cat $f/machine-info.json | jq -r '.memory') * 1024))
        usedmem=$(($(cat $f/os/memory.json | jq -r '.used') * 1024))
        freemem=$(($(cat $f/os/memory.json | jq -r '.free') * 1024))
        sharedmem=$(($(cat $f/os/memory.json | jq -r '.shared') * 1024))
        buffersmem=$(($(cat $f/os/memory.json | jq -r '.buffers') * 1024))
        cachemem=$(($(cat $f/os/memory.json | jq -r '.cache') * 1024))
        availablemem=$(($(cat $f/os/memory.json | jq -r '.available') * 1024))
        cat << EOF > $f/os/meminfo
MemTotal:       ${totalmem} kB
MemFree:        ${freemem} kB
MemAvailable:   ${availablemem} kB
Buffers:         ${buffersmem} kB
Cached:         ${cachemem} kB
SwapCached:            0 kB
Active:                0 kB
Inactive:              0 kB
Active(anon):          0 kB
Inactive(anon):        0 kB
Active(file):          0 kB
Inactive(file):        0 kB
Unevictable:           0 kB
Mlocked:               0 kB
SwapTotal:             0 kB
SwapFree:              0 kB
Dirty:                 0 kB
Writeback:             0 kB
AnonPages:             0 kB
Mapped:                0 kB
Shmem:                 0 kB
Slab:                  0 kB
SReclaimable:          0 kB
SUnreclaim:            0 kB
KernelStack:           0 kB
PageTables:            0 kB
NFS_Unstable:          0 kB
Bounce:                0 kB
WritebackTmp:          0 kB
CommitLimit:           0 kB
Committed_AS:          0 kB
VmallocTotal:          0 kB
VmallocUsed:           0 kB
VmallocChunk:          0 kB
AnonHugePages:         0 kB
ShmemHugePages:        0 kB
ShmemPmdMapped:        0 kB
HugePages_Total:       0
HugePages_Free:        0
HugePages_Rsvd:        0
HugePages_Surp:        0
Hugepagesize:          0 kB
DirectMap4k:           0 kB
DirectMap2M:           0 kB
DirectMap1G:           0 kB
EOF
fi
done

for f in $BASE_DIR/extracted/*; do
    if [[ ! -f "$f/java-version.txt" ]];
    then
        version=$(cat $f/java_system_properties.json | jq -r '."java.version"')
        build=$(cat $f/java_system_properties.json | jq -r '."java.runtime.version"')
        hotspot=$(cat  $f/java_system_properties.json | jq -r '."java.vm.version"')
        cat << EOF > $f/java-version.txt
java version "${version}"
Java(TM) SE Runtime Environment (build ${build})
Java HotSpot(TM) 64-Bit Server VM (build ${hotspot}, mixed mode)
EOF
fi
done