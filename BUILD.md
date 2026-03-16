# Building Montecristo

`build.sh` compiles and installs the **Montecristo** analysis tool and the optional **DSE stats converter** from source using Gradle.

## Prerequisites

| Requirement | Version |
|---|---|
| Java (JDK) | 11 (required) |
| Gradle | Provided via the included Gradle wrapper (`./gradlew`) |

### Installing Java 11 on Apple Silicon (M1/M2/M3)

This project requires Java 11 (JDK 11). On Apple Silicon Macs, you need a native arm64 build of Java 11 for optimal performance.

**Recommended: Adoptium Temurin 11 (arm64 native)**

Install via Homebrew:
```bash
brew install --cask temurin@11
```

**Verify Installation:**
```bash
/usr/libexec/java_home -v 11
# Should output: /Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home

java -version
# Should show: openjdk version "11.0.x"
```

**Alternative Options:**
- **Azul Zulu 11**: `brew install --cask zulu@11` (arm64 native)
- **Amazon Corretto 11**: `brew install --cask corretto11` (arm64 native)

**Note:** Avoid x86_64 (Intel) Java builds like IBM Semeru on Apple Silicon, as they run under Rosetta 2 emulation and may cause compatibility issues.

### Manual JAVA_HOME Setup

If the build script doesn't automatically detect Java 11, set `JAVA_HOME` manually:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
./build.sh
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
| `-c` | Clean all build artifacts before building. Also removes any previously extracted DSE jars from `dse-stats-converter/libs/`. |
| `-d DSE_TARBALL` | Path to a DSE binary tarball (e.g. `dse-6.8.x-bin.tar.gz`). Required jars will be extracted into `dse-stats-converter/libs/` automatically. See [DSE Stats Converter](#dse-stats-converter) below. |
| `-t` | Run tests on all projects after building. |
| `-h` | Print help and exit. |

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

**Build to default Gradle output locations (no destination specified):**
```bash
./build.sh
# Installs to:
#   montecristo/build/install/montecristo/bin/montecristo
#   dse-stats-converter/build/install/dse-stats-converter/bin/dse-stats-converter
```

## DSE Stats Converter

The `dse-stats-converter` tool converts binary SSTable statistics files produced by DSE into a format Montecristo can read. It requires proprietary DSE jar files that are **not included** in this repository.

To provide them, pass the `-d` flag with the path to a DSE binary tarball. The following jars will be extracted from `<dse-dir>/resources/cassandra/lib/` inside the tarball:

- `dse-db-all-*.jar`
- `dse-commons-*.jar`
- `durian-*.jar`
- `jctools-core-*.jar`
- `rxjava-2.*.jar`
- `netty-all-*.jar`
- `agrona-*.jar`

If the DSE jars are not available, the `dse-stats-converter` build will fail with a warning and DSE SSTable statistics conversion will be unavailable. Montecristo itself will still build and run successfully.