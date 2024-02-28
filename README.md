# Discovery Analysis Tools

We have developed Montecristo, an analysis tool to help with discovery of a Cassandra / DSE Cluster.

Installation and setup found at [bottom of document](#InstallAndSetup).

## Grab & Analysis

When running discovery for the first time - if you are using an AWS dead-drop bucket, navigate to `tlp-tools/discovery/` and run the following commands:

    # copy and fill out your aws credentials
    cp pull/env/secrets/aws.env.template pull/env/secrets/aws.env
    vi pull/env/secrets/aws.env


If using an AWS dead drop bucket, please edit the aws-env.sh file and put the bucket name into the file:

    export COLLECTOR_S3_BUCKET=your-collector-dead-drop

---

**Arguments**:  
**`BUCKET_ISSUE_FOLDER`**

Is the folder in the configured _collector-dead-drop_ bucket containing the archives uploaded by the collector. This will be either the ticket id assigned when building the collector combined with the timestamp the collector was run.

**`ENCRYPTION_KEY`**

During the collector build process, an encryption key is generated in the format `<PROJECT_ID>_secret.key`. When used during collection, the output files will have a .enc extension on them. The argument only needs to be provided when that extension is shown.

---

Existing bucket issue folders can be listed using the following command:

    ./run.sh -l

Then, run the following command to trigger the analysis:

    # run the analysis
    ./run.sh $BUCKET_ISSUE_FOLDER $ENCRYPTION_KEY

The above will:  

- Download the encrypted artifacts to `~/ds-discovery/<ISSUE-ID>/artifacts`
- Decrypt them (`bad decrypt` errors can be ignored as there's a fallback method that triggers automatically)
- Decompress the artifacts to `~/ds-discovery/<ISSUE-ID>/extracted`
- Generate the metrics sqlite db from the extracted `metrics.jmx` files under `~/ds-discovery/<ISSUE-ID>/metrics.db`. This needs to be generated only once, but can be overwritten if need be on subsequent runs
- Recompile Montecristo
- Perform the Montecristo analysis and generate a markdown report
- Start a Hugo server with the report

If there is a need to re-run the analysis, after a modification was made in Montecristo for example, you can avoid re-downloading the artifacts from the S3 bucket using the `-e` flag:  

    ./run.sh -e $BUCKET_ISSUE_FOLDER $ENCRYPTION_KEY


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
6. To enable the conversion and reading of Datastax Enterprise sstablemetadata files, the licensed Datastax Enterprise 6.8 should be downloaded from [Datastax Enterprise](https://www.datastax.com/products/datastax-enterprise/download) and the following jars extracted from the download and placed into the dse-stats-converter/libs folder:
   * agrona-0.9.26.jar
   * dse-commons-6.8.17.jar
   * dse-db-all-6.8.17.jar
   * durian-3.4.0.jar
   * jctools-core-2.1.2.jar
   * netty-all-4.1.25.7.dse.jar
   * rxjava-2.2.7.jar
   If the dse-db-all file exists in that folder, it will automatically enable the option within the run.sh script to offer the option of converting the files.