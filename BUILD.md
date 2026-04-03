# Building Montecristo

`build.sh` compiles and installs the **Montecristo** analysis tool and the optional **DSE stats converter** from source using Gradle.

## Prerequisites

| Requirement | Version |
|---|---|
| Java (JDK) | 8 (required) |
| Gradle | Provided via the included Gradle wrapper (`./gradlew`) |

### Installing Java 8 on Apple Silicon (M1/M2/M3)

This project requires Java 8 (JDK 1.8). On Apple Silicon Macs, you need a native arm64 build of Java 8 for optimal performance.

**Recommended: Azul Zulu 8 (arm64 native)**

Install via Homebrew:
```bash
brew install --cask zulu@8
```

After installation, you may need to run the pkg installer to register it with the system:
```bash
open /usr/local/Caskroom/zulu@8/*/zulu-8.jdk/Double-Click\ to\ Install\ Zulu\ 8.pkg
```

**Verify Installation:**
```bash
/usr/libexec/java_home -v 1.8
# Should output: /Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home

java -version
# Should show: openjdk version "1.8.0_..."
```

**Alternative Options:**
- **Adoptium Temurin 8**: `brew install --cask temurin@8` (arm64 native)
- **Amazon Corretto 8**: `brew install --cask corretto8` (arm64 native)

**Note:** Avoid x86_64 (Intel) Java builds like IBM Semeru on Apple Silicon, as they run under Rosetta 2 emulation and may cause compatibility issues.

### Manual JAVA_HOME Setup

If the build script doesn't automatically detect Java 8, set `JAVA_HOME` manually:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
./build.sh
```

## DSE Version Compatibility

**IMPORTANT:** The dse-stats-converter requires **DSE 6.8.x** jars. DSE 6.9 and later versions require Java 11, which is incompatible with this project's Java 8 requirement.

**Supported DSE Versions:**
- DSE 6.8.0 through 6.8.59 (all patch versions)

**Unsupported DSE Versions:**
- DSE 6.9.x and later (requires Java 11)
- DSE 6.7.x and earlier (different jar structure)

When using the `-d` option, ensure you provide a DSE 6.8.x tarball:
```bash
./build.sh -d /path/to/dse-6.8.62-bin.tar.gz
```

## Usage

```
./build.sh [OPTIONS] [DESTINATION_DIR]
```

### Arguments

| Argument | Description |
|---|---|
| `DESTINATION_DIR` | Optional. Directory to install the built binaries into. Two subdirectories will be created inside it: `montecristo/` and `dse-stats-converter/`. If omitted, each project installs to its own default Gradle `installDist` location (`montecristo/build/install/montecristo/` and `dse-stats-converter/build/install/dse-stats-converter/`). |

### Options

| Flag | Description |
|---|---|
| `-c` | Clean build artifacts (does not affect DSE jars). |
| `-d DSE_TARBALL` | Path to a DSE binary tarball (e.g. `dse-6.8.x-bin.tar.gz`). Removes old DSE jars and extracts new ones to `dse-stats-converter/.dse-libs/` (gitignored). Use when first building or upgrading DSE versions. |
| `-D` | Skip building dse-stats-converter. Use when you don't need dse-stats-converter or don't have access to DSE jars. |
| `-O` | Skip building old-c-stats-converter. Use when you don't need old-c-stats-converter. |
| `-t` | Run tests on all projects after building. |
| `-h` | Print help and exit. |

**Note:** DSE jars are stored in `dse-stats-converter/.dse-libs/` (gitignored) and persist between builds. Use `-d` to extract/update them. Build will fail with a helpful message if DSE jars are missing unless `-D` is specified to skip the check.

## Examples

**Build and install to a custom directory:**
```bash
./build.sh ~/tools/datastax
# Installs to:
#   ~/tools/datastax/montecristo/bin/montecristo
#   ~/tools/datastax/dse-stats-converter/bin/dse-stats-converter
```

**Build with DSE jar extraction:**
```bash
./build.sh -d /path/to/dse-6.8.62-bin.tar.gz ~/tools/datastax
```

**Clean rebuild:**
```bash
./build.sh -c ~/tools/datastax
```

**Build with tests:**
```bash
./build.sh -t ~/tools/datastax
```

**Clean rebuild with tests:**
```bash
./build.sh -c -t ~/tools/datastax
```

**Build without dse-stats-converter (skip DSE jar check):**
```bash
./build.sh -D ~/tools/datastax
# Only builds montecristo and old-c-stats-converter
```

**Build to default Gradle output locations (no destination specified):**
```bash
./build.sh
# Installs to:
#   montecristo/build/install/montecristo/bin/montecristo
#   dse-stats-converter/build/install/dse-stats-converter/bin/dse-stats-converter
```

## DSE Stats Converter

The `dse-stats-converter` tool converts binary SSTable statistics files produced by DSE into a format Montecristo can read. It requires proprietary DSE jar files that are **not included** in this repository.

### Providing DSE Dependencies

Pass the `-d` flag with the path to a DSE binary tarball. The build script will automatically extract the required jars from `<dse-dir>/resources/cassandra/lib/` into `dse-stats-converter/libs/`.

Example:
```bash
./build.sh -d ~/Downloads/dse-6.8.62-bin.tar.gz
```

If the DSE jars are not available, the `dse-stats-converter` build will fail with a warning and DSE SSTable statistics conversion will be unavailable. Montecristo itself will still build and run successfully.

### How Dependencies Were Determined

The minimal set of runtime dependencies was identified through **iterative testing** rather than static analysis:

1. **Initial Analysis**: Used `jdeps` to analyze `dse-db-all-6.8.62.jar`, which revealed 838 external class references across the entire codebase.

2. **Runtime Testing**: Since `dse-stats-converter` only uses `org.apache.cassandra.tools.SSTableMetadataViewer`, we tested with progressively larger classpaths:
   - Started with core jars (dse-db-all, netty, guava, slf4j, logback)
   - Added dependencies one-by-one as `ClassNotFoundException` errors occurred
   - Verified successful initialization when the tool showed its help message

3. **Key Discovery**: `netty-all-4.1.128.1.dse.jar` is only 4KB - it's a POM aggregator, not a fat jar. DSE requires all 30+ individual netty module jars (buffer, codec, handler, transport, etc.) including the custom `netty-transport-classes-epoll` with DSE's `Aio` classes.

4. **Final Verification**: Tested the tool runs successfully with Java 8 and initializes all DSE classes without errors.

### Extracted Dependencies (22 patterns, ~50 jars)

The build script extracts these jar patterns from the DSE tarball (note: `netty-all` is just a POM aggregator, all individual `netty-*.jar` modules are needed):

- `agrona-*.jar`
- `caffeine-*.jar`
- `commons-cli-*.jar`
- `commons-codec-*.jar`
- `commons-io-*.jar`
- `commons-lang3-*.jar`
- `commons-math3-*.jar`
- `dse-commons-*.jar`
- `dse-db-all-*.jar`
- `durian-*.jar`
- `guava-*.jar`
- `HdrHistogram-*.jar`
- `jamm-*.jar`
- `jctools-core-*.jar`
- `jna-*.jar`
- `joda-time-*.jar`
- `logback-classic-*.jar`
- `logback-core-*.jar`
- `metrics-core-*.jar`
- `netty-*.jar`
- `reactive-streams-*.jar`
- `rxjava-*.jar`
- `slf4j-api-*.jar`
- `stream-*.jar`

This minimal set (24 patterns) ensures the tool works while avoiding unnecessary dependencies (cloud storage SDKs, Scala libraries, etc.) that would have been included from a full `jdeps` analysis.

**Note**: `jna-*.jar` (Java Native Access) is required to prevent `UnsatisfiedLinkError` when UUIDGen's static initializer tries to call native methods for process ID retrieval.

### Regression Protection

The `ConvertTest` in dse-stats-converter now verifies that all Statistics.db files are successfully converted to .txt files. If any required dependencies are missing, the test will fail with a clear error message. This provides automatic regression protection during builds to catch missing dependencies early.

The test processes 3 real Statistics.db files from the shared montecristo test resources and verifies that output files are created. This ensures all runtime dependencies (including the critical `stream-*.jar` for clearspring analytics) are present and functional.