# Agent Development Notes

## Building the Project

### Java Version Requirement
All projects (montecristo, dse-stats-converter, old-c-stats-converter) require **Java 11 (JDK 11)**.

### Setting JAVA_HOME for Gradle Builds

When running Gradle commands directly (not through build.sh), you must set JAVA_HOME to Java 11:

**macOS:**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
cd old-c-stats-converter
./gradlew clean build -x test
```

**Linux:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Debian/Ubuntu
# or
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk        # RHEL/CentOS
cd old-c-stats-converter
./gradlew clean build -x test
```

### Using build.sh (Recommended)

The `build.sh` script automatically finds and sets Java 11 on all platforms, so it's the preferred method:

```bash
./build.sh -c  # Clean build all projects
./build.sh -t  # Build all projects and run tests
./build.sh -c -t  # Clean build all projects and run tests
./build.sh -d /path/to/dse-*.tar.gz /path/to/install  # Build with DSE jars
```

The script uses a multi-step approach to find Java 11:
1. Checks if JAVA_HOME is already set to Java 11
2. Tries `/usr/libexec/java_home -v 11` (macOS only, if available)
3. Checks common Java 11 installation paths on macOS and Linux (prioritizing Temurin over Semeru)

This means build.sh works cross-platform without requiring the macOS-specific `java_home` command.

## Project Structure

### Three Main Components

1. **montecristo** - Main analysis tool
   - Generates metrics DB and discovery reports

2. **dse-stats-converter** - DSE SSTable statistics converter
   - Requires DSE jars (extracted from DSE tarball)

3. **old-c-stats-converter** - Old Cassandra (2.2) statistics converter
   - Converts old Cassandra 2.2 Statistics.db files to text format

## Common Issues

### "Unsupported class file major version 65"
This error occurs when Gradle tries to use Java 21 instead of Java 11.
**Solution**: Set JAVA_HOME to Java 11 before running Gradle commands.

### IDE Gradle Cache Errors
The IDE may show Gradle cache errors after dependency updates.
**Solution**: These are harmless and will resolve after the next successful build.

### Missing DSE Jars
dse-stats-converter requires DSE jars to build.
**Solution**: Use `./build.sh -d /path/to/dse-*.tar.gz` to extract required jars.