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
DSE_LIBS_DIR="${SCRIPT_DIR}/dse-stats-converter/.dse-libs"
dse_tarball=""

clean_build="false"
run_tests="false"
skip_dse_check="false"
skip_old_c_check="false"

# Function to find Java 8 installation
function find_java8() {
    # Check if JAVA_HOME is already set to Java 8
    if [ -n "${JAVA_HOME}" ]; then
        java_version=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -n 1)
        if echo "${java_version}" | grep -q "1.8"; then
            return 0
        fi
    fi

    # Try to find Java 8 using java_home (macOS)
    if command -v /usr/libexec/java_home &> /dev/null; then
        java8_home=$(/usr/libexec/java_home -v 1.8 2>/dev/null)
        if [ -n "${java8_home}" ] && [ -d "${java8_home}" ]; then
            export JAVA_HOME="${java8_home}"
            export PATH="${JAVA_HOME}/bin:${PATH}"
            echo "Found Java 8 at: ${JAVA_HOME}"
            return 0
        fi
    fi

    # Try common Java 8 installation locations
    java8_locations=(
        "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home"
        "/usr/lib/jvm/java-8-openjdk"
        "/usr/lib/jvm/java-1.8.0-openjdk"
    )

    for location in "${java8_locations[@]}"; do
        if [ -d "${location}" ]; then
            export JAVA_HOME="${location}"
            export PATH="${JAVA_HOME}/bin:${PATH}"
            echo "Found Java 8 at: ${JAVA_HOME}"
            return 0
        fi
    done

    return 1
}

# Function to check if DSE libs are present
function check_dse_libs() {
    if [ ! -d "${DSE_LIBS_DIR}" ]; then
        return 1
    fi
    
    # Check for at least one required jar (dse-db-all is always needed)
    if ! ls "${DSE_LIBS_DIR}"/dse-db-all-*.jar 1> /dev/null 2>&1; then
        return 1
    fi
    
    return 0
}

# Ensure Java 8 is available
echo "Checking for Java 8..."
if ! find_java8; then
    echo "Error: Java 8 (JDK 1.8) is required but not found."
    echo ""
    echo "Please install Java 8. For Apple Silicon Macs, we recommend Azul Zulu 8:"
    echo "  brew install --cask zulu@8"
    echo ""
    echo "After installation, you may need to run the pkg installer to register it:"
    echo "  open /usr/local/Caskroom/zulu@8/*/zulu-8.jdk/Double-Click\\ to\\ Install\\ Zulu\\ 8.pkg"
    echo ""
    echo "Or set JAVA_HOME manually before running this script:"
    echo "  export JAVA_HOME=/path/to/java8"
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
 -c               Clean build artifacts (does not affect DSE jars).
 -d DSE_TARBALL   Path to a DSE binary tarball (e.g. dse-6.8.x-bin.tar.gz).
                  Removes old DSE jars and extracts new ones to dse-stats-converter/.dse-libs/.
                  Use when first building or upgrading DSE versions.
 -D               Skip building dse-stats-converter.
                  Use when you don't need dse-stats-converter or don't have access to DSE jars.
 -O               Skip building old-c-stats-converter.
                  Use when you don't need old-c-stats-converter.
 -t               Run tests on all projects after building.
 -h               Help and usage.

Note: DSE jars are stored in dse-stats-converter/.dse-libs/ (gitignored) and persist
      between builds. Use '-d' to extract/update them. Build will fail with a helpful
      message if DSE jars are missing unless '-D' is specified to skip the check.
EOF
    exit 2
}

while getopts "cd:DOth" opt_flag; do
    case $opt_flag in
        c) clean_build="true" ;;
        d) dse_tarball=$OPTARG ;;
        D) skip_dse_check="true" ;;
        O) skip_old_c_check="true" ;;
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

# Check if DSE libs are present before doing anything (unless -d or -D is specified)
if [ "${skip_dse_check}" = "false" ] && [ -z "${dse_tarball}" ] && [ -f "${SCRIPT_DIR}/dse-stats-converter/build.gradle" ]; then
    if ! check_dse_libs; then
        echo ""
        echo "ERROR: DSE jars not found in ${DSE_LIBS_DIR}/"
        echo ""
        echo "dse-stats-converter requires DSE jars to build and run."
        echo "Extract them using:"
        echo "  ./build.sh -d /path/to/dse-*.tar.gz"
        echo ""
        echo "Or skip dse-stats-converter build with:"
        echo "  ./build.sh -D"
        echo ""
        echo "Example:"
        echo "  ./build.sh -d ~/Downloads/dse-6.8.63-bin.tar.gz"
        echo ""
        exit 1
    fi
fi

# Clean build artifacts if requested (never touches DSE libs)
# When -c is specified, clean ALL projects regardless of -D or -O flags
if [ "${clean_build}" = "true" ]; then
    echo "Cleaning build artifacts for all projects..."
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
    echo "Clean complete."
    echo
fi

# Function to check if DSE libs are present
function check_dse_libs() {
    if [ ! -d "${DSE_LIBS_DIR}" ]; then
        return 1
    fi
    
    # Check for a few critical jars to verify libs are present
    local critical_jars=("dse-db-all-*.jar" "netty-transport-*.jar" "stream-*.jar")
    for pattern in "${critical_jars[@]}"; do
        if ! ls "${DSE_LIBS_DIR}/"${pattern} 1> /dev/null 2>&1; then
            return 1
        fi
    done
    return 0
}

# Extract DSE jars from tarball if provided
if [ -n "${dse_tarball}" ]; then
    if [ ! -f "${dse_tarball}" ]; then
        echo "Error: DSE tarball not found: ${dse_tarball}"
        exit 1
    fi
    
    # Remove old DSE libs before extracting new ones
    if [ -d "${DSE_LIBS_DIR}" ]; then
        echo "Removing old DSE jars from ${DSE_LIBS_DIR}/"
        rm "${DSE_LIBS_DIR}"/*.jar
    fi
    
    echo "Extracting DSE jars from ${dse_tarball} -> ${DSE_LIBS_DIR}/"
    mkdir -p "${DSE_LIBS_DIR}"

    # Jars needed from resources/cassandra/lib/
    # Minimal set verified by iterative testing with SSTableMetadataViewer
    # Note: netty-all is just a POM aggregator, need all individual netty-*.jar modules
    cassandra_jars=(
        "agrona-*.jar"
        "caffeine-*.jar"
        "commons-cli-*.jar"
        "commons-codec-*.jar"
        "commons-io-*.jar"
        "commons-lang3-*.jar"
        "commons-math3-*.jar"
        "dse-commons-*.jar"
        "dse-db-all-*.jar"
        "durian-*.jar"
        "guava-*.jar"
        "HdrHistogram-*.jar"
        "jamm-*.jar"
        "jctools-core-*.jar"
        "jna-*.jar"
        "joda-time-*.jar"
        "logback-classic-*.jar"
        "logback-core-*.jar"
        "metrics-core-*.jar"
        "netty-*.jar"
        "reactive-streams-*.jar"
        "rxjava-*.jar"
        "slf4j-api-*.jar"
        "stream-*.jar"
    )

    for pattern in "${cassandra_jars[@]}"; do
        # Find matching entries in the tarball
        matches=$(tar -tzf "${dse_tarball}" | grep "resources/cassandra/lib/${pattern//\*/.*}" 2>/dev/null)
        if [ -n "${matches}" ]; then
            # Extract all matching jars
            while IFS= read -r match; do
                echo "  Extracting: ${match}"
                # strip-components=4 removes: <dse-dir>/resources/cassandra/lib/ leaving just the jar filename
                tar -xzf "${dse_tarball}" -C "${DSE_LIBS_DIR}" --strip-components=4 "${match}"
            done <<< "${matches}"
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

if [ "${skip_dse_check}" = "false" ]; then
    if [ -f "${SCRIPT_DIR}/dse-stats-converter/build.gradle" ]; then
        echo "Building dse-stats-converter -> ${DSE_STATS_INSTALL_DIR}"
        pushd "${SCRIPT_DIR}/dse-stats-converter" || exit 1
        if [ "${run_tests}" = "true" ]; then
            ./gradlew installDist test -PinstallPath="${DSE_STATS_INSTALL_DIR}" || exit 1
        else
            ./gradlew installDist -x test -PinstallPath="${DSE_STATS_INSTALL_DIR}" || exit 1
        fi
        popd || exit 1
        echo "  dse-stats-converter: ${DSE_STATS_INSTALL_DIR}/bin/dse-stats-converter"
    else
        echo "dse-stats-converter source not found, skipping."
    fi
else
    echo "Skipping dse-stats-converter build (-D specified)"
fi

if [ "${skip_old_c_check}" = "false" ]; then
    if [ -f "${SCRIPT_DIR}/old-c-stats-converter/build.gradle" ]; then
        echo "Building old-c-stats-converter -> ${OLD_C_STATS_INSTALL_DIR}"
        pushd "${SCRIPT_DIR}/old-c-stats-converter" || exit 1
        if [ "${run_tests}" = "true" ]; then
            ./gradlew installDist test -PinstallPath="${OLD_C_STATS_INSTALL_DIR}" || exit 1
        else
            ./gradlew installDist -x test -PinstallPath="${OLD_C_STATS_INSTALL_DIR}" || exit 1
        fi
        popd || exit 1
        echo "  old-c-stats-converter: ${OLD_C_STATS_INSTALL_DIR}/bin/old-c-stats-converter"
    else
        echo "old-c-stats-converter source not found, skipping."
    fi
else
    echo "Skipping old-c-stats-converter build (-O specified)"
fi

echo
echo "Build complete!"
echo "  montecristo: ${MONTECRISTO_INSTALL_DIR}/bin/montecristo"