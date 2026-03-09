# Discovery Analysis Tools

Montecristo is an analysis tool to help with discovery of a Cassandra / HCD / DSE Cluster.

It generates comprehensive Health Discovery reports from diagnostics collected from the [Diagnostic Collector for Apache Cassandra, DSE, HCD](https://github.com/datastax/diagnostic-collection/).

Installation and setup found at [bottom of document](#InstallAndSetup).

## Analysis

**Arguments**:
**`ISSUE_FOLDER`**

The folder name that will be created under `~/ds-discovery/` to store artifacts and analysis results. This is typically a ticket ID or identifier for the cluster being analyzed.

**`ENCRYPTION_KEY_PATH`** (optional)

Path to the encryption key file. Some diagnostic-collection bundles come with an encryption key in the format `<PROJECT_ID>_secret.key`. When used during collection, the output files will have a .enc extension on them. The argument only needs to be provided when encrypted artifacts are present.

---

### Running Analysis with Local Artifacts

To analyze artifacts from a local directory:

    # Copy artifacts from local directory
    ./run.sh -c /path/to/local/artifacts $ISSUE_FOLDER $ENCRYPTION_KEY_PATH

Or if artifacts are already in `~/ds-discovery/$ISSUE_FOLDER/artifacts`:

    # Run analysis on existing artifacts
    ./run.sh $ISSUE_FOLDER $ENCRYPTION_KEY_PATH

The above will:

- Decrypt the artifacts if encrypted (`bad decrypt` errors can be ignored as there's a fallback method that triggers automatically)
- Decompress the artifacts to `~/ds-discovery/<ISSUE_FOLDER>/extracted`
- Generate the metrics sqlite db from the extracted `metrics.jmx` files under `~/ds-discovery/<ISSUE_FOLDER>/metrics.db`. This needs to be generated only once, but can be overwritten if need be on subsequent runs
- Recompile Montecristo
- Perform the Montecristo analysis and generate a markdown report
- Start a Hugo server with the report

If there is a need to re-run the analysis after a modification was made in Montecristo, use the `-e` flag to skip download and extraction:

    ./run.sh -e $ISSUE_FOLDER $ENCRYPTION_KEY_PATH


Once the analysis is done, you should see the list of generated recommendations in the console output and the following lines:  

```
Environment: "development"
Serving pages from memory
Running in Fast Render Mode. For full rebuilds on change: hugo server --disableFastRender
Web Server is available at http://localhost:1313/ (bind address 127.0.0.1)
Press Ctrl+C to stop
```


## Document Generation

The Montecristo analysis creates folders named after time it runs and places them in the reports directory: 

```
$ ls -l reports/discovery_$date
drwxr-xr-x  18 zvo  staff  576 Jan  8 16:14 2018-01-08-13-51-59
```

Each of the run directories contains some markdown files:

```
montecristo % ls -l /Users/andrew.hogg/ds-discovery/TEST-123/reports/montecristo/content
total 2320
-rw-r--r--  1 andrew.hogg  staff   10028 24 May 13:24 001_summary.md
-rw-r--r--  1 andrew.hogg  staff     201 24 May 13:24 002_introduction.md
-rw-r--r--  1 andrew.hogg  staff      26 24 May 13:24 003_infrastructure_heading.md
-rw-r--r--  1 andrew.hogg  staff    1606 24 May 13:24 004_infrastructure_overview.md
-rw-r--r--  1 andrew.hogg  staff     260 24 May 13:24 005_infrastructure_path.md
-rw-r--r--  1 andrew.hogg  staff    1376 24 May 13:24 006_infrastructure_storage.md
-rw-r--r--  1 andrew.hogg  staff     149 24 May 13:24 007_infrastructure_swap.md
-rw-r--r--  1 andrew.hogg  staff    1401 24 May 13:24 008_infrastructure_ntp.md
-rw-r--r--  1 andrew.hogg  staff     579 24 May 13:24 009_infrastructure_java_version.md
-rw-r--r--  1 andrew.hogg  staff    1426 24 May 13:24 010_infrastructure_os_config_overview.md
-rw-r--r--  1 andrew.hogg  staff      20 24 May 13:24 011_configuration_heading.md
-rw-r--r--  1 andrew.hogg  staff     384 24 May 13:24 012_configuration_cassandra_version.md
-rw-r--r--  1 andrew.hogg  staff     751 24 May 13:24 013_configuration_custom_settings.md
```

Once the analysis process completes:
- Hugo launches the webserver for the results.
- Open [http://localhost:1313/final/](http://localhost:1313/final/) in your browser; should contain the freshly baked discovery document.
- Hit Cmd-A and Cmd-C to copy the whole content of the report.
- Create a new document in Google Docs or an editor of your choice with your chosen template.
- Paste the content into the new document.
- Wait until the paste completes (it can take a while, let the browser work).

Inspect all sections thoroughly and rule out any recommendation/section that doesn't seem relevant.

The discovery document should be completed with the schema analysis and reviewed carefully.

## View Report Manually

If you need to view the generated report again, you can run `hugo server` in the `discovery_$date` directory of given date:

```
/Users/andrew.hogg/ds-discovery/TEST-123/reports/montecristo % hugo server
Start building sites …
hugo v0.94.2+extended darwin/amd64 BuildDate=unknown

                   | EN
-------------------+-----
  Pages            | 78
  Paginator pages  |  0
  Non-page files   |  0
  Static files     |  8
  Processed images |  0
  Aliases          |  0
  Sitemaps         |  1
  Cleaned          |  0

Built in 184 ms
Watching for changes in /Users/andrew.hogg/ds-discovery/TEST-123/reports/montecristo/{content,layouts,themes}
Watching for config changes in /Users/andrew.hogg/ds-discovery/TEST-123/reports/montecristo/config.toml
Environment: "development"
Serving pages from memory
Running in Fast Render Mode. For full rebuilds on change: hugo server --disableFastRender
Web Server is available at http://localhost:1313/ (bind address 127.0.0.1)
Press Ctrl+C to stop
```

---
## <a name="InstallAndSetup"></a>Installing and Setup

These instructions tested on new/clean Windows install using WSL2 running Ubuntu 20.04. It has *not* been validated in Windows directly and is unlikely to work...

1. [OpenJDK 8 with HotSpot ](https://adoptium.net/releases.html?variant=openjdk8&jvmVariant=hotspot) must be the default `java`
    * On Ubuntu: `sudo apt install openjdk-8-jdk`
    * On OSX: `brew tap homebrew/cask-versions` then `brew install --cask temurin8`
2. [Docker Desktop](https://www.docker.com/products/docker-desktop) must be installed
3. Current user must be a member of the `docker` group
    * On Ubuntu: `sudo usermod -aG docker $(whoami)`
4. `hugo` and `zip` must be installed
    * On Ubuntu: `sudo apt install hugo zip`
    * On OSX: `brew install hugo`, `brew install zip` and `brew install unzip`
5. jq should be installed
   * On Ubuntu: `sudo apt install jq`
   * On OSX: `brew install jq`
6. To enable the conversion and reading of Datastax Enterprise sstablemetadata files, download IBM DataStax Enterprise from [IBM Fix Central](https://www.ibm.com/support/fixcentral/options?selectionBean.selectedTab=find&selection=ibm%2fInformation+Management%3bibm%2fInformation+Management%2fIBM+DataStax+Enterprise) and use the `-d` flag with `build.sh` to automatically extract the required jars from the DSE tarball:
   ```bash
   ./build.sh -d /path/to/dse-6.8.x-bin.tar.gz
   ```
   This will extract the following jars from the DSE tarball into `dse-stats-converter/libs/`:
   * agrona-*.jar
   * dse-commons-*.jar
   * dse-db-all-*.jar
   * durian-*.jar
   * jctools-core-*.jar
   * netty-all-*.jar
   * rxjava-2.*.jar
   
   If the dse-db-all file exists in that folder, it will automatically enable the option within the run.sh script to offer the option of converting the files.
7. When running for the first time, execute the `mkhugozip.sh` script in `montecristo/src/main/resources/`

## License and Copyright

All contents in, and contributions to, this repository are copyrighted and licensed as:

&copy; DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
