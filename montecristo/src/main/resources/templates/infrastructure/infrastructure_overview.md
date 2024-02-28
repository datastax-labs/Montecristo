## General Overview

[CUSTOMER-NAME] is running cluster `{{clusterName}}` on [CLOUD-OR-ONPREM]

The [CLUSTER-NAME] cluster is used for [CLUSTER-USAGE-DESCRIPTION]

It contains the following nodes:

{{infra}}

{{#showAWS}}

## AWS Usage

### Instances By Availability Zone and Region

Within each region there are multiple availability zones.  We recommend spreading nodes in a cluster evenly across AZs in order to avoid downtime.  Additionally, we recommend using a consistent image size, AMI, and configuration for machines within a given DC to ensure consistent performance and minimize operational overhead.  

{{awsInstances}}
 
{{/showAWS}}


