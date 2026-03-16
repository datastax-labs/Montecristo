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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DSE_LIBS_DIR="${SCRIPT_DIR}/dse-stats-converter/libs"
dse_tarball=""

clean_build="false"
run_tests="false"

# Function to find Java 11 installation
function find_java11() {
    # Check if JAVA_HOME is already set to Java 11
    if [ -n "${JAVA_HOME}" ]; then
        java_version=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -n 1)
        if echo "${java_version}" | grep -q "\"11\."; then
            return 0
        fi
    fi

    # Try common Java 11 installation locations first (prioritize Temurin and OpenJDK-based over Semeru)
    java11_locations=(
        "/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/corretto-11.jdk/Contents/Home"
        "/usr/lib/jvm/java-11-openjdk"
        "/usr/lib/jvm/java-11-openjdk-amd64"
        "/Library/Java/JavaVirtualMachines/ibm-semeru-open-11.jdk/Contents/Home"
    )

    for location in "${java11_locations[@]}"; do
        if [ -d "${location}" ]; then
            export JAVA_HOME="${location}"
            export PATH="${JAVA_HOME}/bin:${PATH}"
            echo "Found Java 11 at: ${JAVA_HOME}"
            return 0
        fi
    done

    # Fall back to java_home if specific locations not found (macOS)
    if command -v /usr/libexec/java_home &> /dev/null; then
        java11_home=$(/usr/libexec/java_home -v 11 2>/dev/null)
        if [ -n "${java11_home}" ] && [ -d "${java11_home}" ]; then
            export JAVA_HOME="${java11_home}"
            export PATH="${JAVA_HOME}/bin:${PATH}"
            echo "Found Java 11 at: ${JAVA_HOME}"
            return 0
        fi
    fi

    return 1
}

# Ensure Java 11 is available
echo "Checking for Java 11..."
if ! find_java11; then
    echo "Error: Java 11 (JDK 11) is required but not found."
    echo ""
    echo "Please install Java 11. For Apple Silicon Macs, we recommend Azul Zulu 11:"
    echo "  brew install --cask zulu@11"
    echo ""
    echo "After installation, you may need to run the pkg installer to register it:"
    echo "  open /usr/local/Caskroom/zulu@11/*/zulu-11.jdk/Double-Click\\ to\\ Install\\ Zulu\\ 11.pkg"
    echo ""
    echo "Or set JAVA_HOME manually before running this script:"
    echo "  export JAVA_HOME=/path/to/java11"
    echo "  ./build.sh"
    exit 1
fi

# Verify Java version
java_version=$(java -version 2>&1 | head -n 1)
echo "Using Java: ${java_version}"
echo "JAVA_HOME: ${JAVA_HOME}"
echo ""

function usage() {
    cat << EOF
Builds and installs the Montecristo and stats converter binaries.

usage: ./build.sh [OPTIONS] [DESTINATION_DIR]

DESTINATION_DIR  Optional. Directory to install binaries into.
                 Three subdirectories will be created:
                   <DESTINATION_DIR>/montecristo/
                   <DESTINATION_DIR>/dse-stats-converter/
                   <DESTINATION_DIR>/old-c-stats-converter/
                 Defaults to each project's standard gradle installDist location:
                   montecristo/build/install/montecristo/
                   dse-stats-converter/build/install/dse-stats-converter/
                   old-c-stats-converter/build/install/old-c-stats-converter/

Options:
 -c               Clean all build artifacts before building.
 -d DSE_TARBALL   Path to a DSE binary tarball (e.g. dse-6.8.x-bin.tar.gz).
                  Required jars will be extracted into dse-stats-converter/libs/.
 -t               Run tests on all projects after building.
 -h               Help and usage.
EOF
    exit 2
}

while getopts "cd:th" opt_flag; do
    case $opt_flag in
        c) clean_build="true" ;;
        d) dse_tarball=$OPTARG ;;
        t) run_tests="true" ;;
        h) usage ;;
        *) usage ;;
    esac
done

shift $(($OPTIND - 1))

if [ "${1}" = "-h" ] || [ "${1}" = "--help" ]; then
    usage
fi

if [ -n "${1}" ]; then
    DEST_DIR=$(mkdir -p "$1" && cd "$1" && pwd)
    if [ -z "${DEST_DIR}" ]; then
        echo "Error: could not create destination directory '$1'"
        exit 1
    fi
    MONTECRISTO_INSTALL_DIR="${DEST_DIR}/montecristo"
    DSE_STATS_INSTALL_DIR="${DEST_DIR}/dse-stats-converter"
    OLD_C_STATS_INSTALL_DIR="${DEST_DIR}/old-c-stats-converter"
else
    MONTECRISTO_INSTALL_DIR="${SCRIPT_DIR}/montecristo/build/install/montecristo"
    DSE_STATS_INSTALL_DIR="${SCRIPT_DIR}/dse-stats-converter/build/install/dse-stats-converter"
    OLD_C_STATS_INSTALL_DIR="${SCRIPT_DIR}/old-c-stats-converter/build/install/old-c-stats-converter"
fi

# Clean build artifacts if requested
if [ "${clean_build}" = "true" ]; then
    echo "Cleaning build artifacts..."
    pushd "${SCRIPT_DIR}/montecristo" || exit 1
    ./gradlew clean || exit 1
    popd || exit 1

    if [ -f "${SCRIPT_DIR}/dse-stats-converter/build.gradle" ]; then
        pushd "${SCRIPT_DIR}/dse-stats-converter" || exit 1
        ./gradlew clean || exit 1
        popd || exit 1
    fi

    if [ -f "${SCRIPT_DIR}/old-c-stats-converter/build.gradle" ]; then
        pushd "${SCRIPT_DIR}/old-c-stats-converter" || exit 1
        ./gradlew clean || exit 1
        popd || exit 1
    fi

    # Also remove extracted DSE libs so they get re-extracted if -d is provided
    if [ -d "${DSE_LIBS_DIR}" ]; then
        echo "Removing ${DSE_LIBS_DIR}..."
        rm -rf "${DSE_LIBS_DIR}"
    fi
    echo "Clean complete."
    echo
fi

# Extract DSE jars from tarball if provided
if [ -n "${dse_tarball}" ]; then
    if [ ! -f "${dse_tarball}" ]; then
        echo "Error: DSE tarball not found: ${dse_tarball}"
        exit 1
    fi
    echo "Extracting DSE jars from ${dse_tarball} -> ${DSE_LIBS_DIR}/"
    mkdir -p "${DSE_LIBS_DIR}"

    # Jars needed from resources/cassandra/lib/
    # Note: some dependencies are included (override) via `montecristo/build.gradle` 
    cassandra_jars=(
        "dse-db-all-*.jar"
        "dse-commons-*.jar"
        "durian-*.jar"
        "jctools-core-*.jar"
        "rxjava-2.*.jar"
        "agrona-*.jar"
    )

    for pattern in "${cassandra_jars[@]}"; do
        # Find matching entries in the tarball
        matches=$(tar -tzf "${dse_tarball}" | grep "resources/cassandra/lib/${pattern//\*/.*}" 2>/dev/null | head -1)
        if [ -n "${matches}" ]; then
            echo "  Extracting: ${matches}"
            # strip-components=4 removes: <dse-dir>/resources/cassandra/lib/ leaving just the jar filename
            tar -xzf "${dse_tarball}" -C "${DSE_LIBS_DIR}" --strip-components=4 "${matches}"
        else
            echo "  Warning: no match for pattern '${pattern}' in resources/cassandra/lib/"
        fi
    done

    echo "Done extracting DSE jars."
    echo
fi

echo "Building montecristo -> ${MONTECRISTO_INSTALL_DIR}"

# Create hugo.zip if it doesn't exist
HUGO_ZIP="${SCRIPT_DIR}/montecristo/src/main/resources/hugo.zip"
if [ ! -f "${HUGO_ZIP}" ]; then
    echo "Creating hugo.zip from hugo directory..."
    pushd "${SCRIPT_DIR}/montecristo/src/main/resources" || exit 1
    ./mkhugozip.sh || exit 1
    popd || exit 1
    echo "hugo.zip created successfully."
fi

pushd "${SCRIPT_DIR}/montecristo" || exit 1
if [ "${run_tests}" = "true" ]; then
    ./gradlew installDist test -PinstallPath="${MONTECRISTO_INSTALL_DIR}" || exit 1
else
    ./gradlew installDist -x test -PinstallPath="${MONTECRISTO_INSTALL_DIR}" || exit 1
fi
popd || exit 1

if [ -f "${SCRIPT_DIR}/dse-stats-converter/build.gradle" ]; then
    echo "Building dse-stats-converter -> ${DSE_STATS_INSTALL_DIR}"
    pushd "${SCRIPT_DIR}/dse-stats-converter" || exit 1
    if [ "${run_tests}" = "true" ]; then
        gradle_cmd="./gradlew installDist test -PinstallPath=\"${DSE_STATS_INSTALL_DIR}\""
    else
        gradle_cmd="./gradlew installDist -x test -PinstallPath=\"${DSE_STATS_INSTALL_DIR}\""
    fi
    if eval ${gradle_cmd}; then
        popd || exit 1
        echo "  dse-stats-converter: ${DSE_STATS_INSTALL_DIR}/bin/dse-stats-converter"
    else
        popd || exit 1
        echo "Warning: dse-stats-converter build failed."
        echo "         If DSE jar files are missing, provide the DSE tarball with: ./build.sh -d /path/to/dse-*.tar.gz"
        echo "         DSE SSTable statistics conversion will be unavailable."
    fi
else
    echo "dse-stats-converter source not found, skipping."
fi

if [ -f "${SCRIPT_DIR}/old-c-stats-converter/build.gradle" ]; then
    echo "Building old-c-stats-converter -> ${OLD_C_STATS_INSTALL_DIR}"
    pushd "${SCRIPT_DIR}/old-c-stats-converter" || exit 1
    if [ "${run_tests}" = "true" ]; then
        gradle_cmd="./gradlew installDist test -PinstallPath=\"${OLD_C_STATS_INSTALL_DIR}\""
    else
        gradle_cmd="./gradlew installDist -x test -PinstallPath=\"${OLD_C_STATS_INSTALL_DIR}\""
    fi
    if eval ${gradle_cmd}; then
        popd || exit 1
        echo "  old-c-stats-converter: ${OLD_C_STATS_INSTALL_DIR}/bin/old-c-stats-converter"
    else
        popd || exit 1
        echo "Warning: old-c-stats-converter build failed."
        echo "         Old Cassandra SSTable statistics conversion will be unavailable."
    fi
else
    echo "old-c-stats-converter source not found, skipping."
fi

echo
echo "Build complete!"
echo "  montecristo: ${MONTECRISTO_INSTALL_DIR}/bin/montecristo"