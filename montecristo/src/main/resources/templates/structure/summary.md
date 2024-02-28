

| **INSTRUCTIONS FOR DELIVERY**                                                                                                                                    |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|                                                                                                                                                                  |
| **DO NOT DELIVER THIS DOCUMENT AS-IS** <br/> **The generated report is always wrong**                                              |
|                                                                                                                                                                  |
| This section describes the actions to complete this document for a client deliverable. <br/> 1.  Open the "[TEMPLATE] Discovery Health Check Report Template" google doc, and File -> Make a copy, saving the copy in a new google drive folder. <br/> 2.  Into the new google doc, copy everything from this ‘Google Document Exporter’ page, paste after the yellow box. <br/> 3.  Update the title page; fill in \[Client Name\] - \[Cluster Name\], \[Month Year\], and \[Author Firstname Lastname\] <br/> 4.  Enter in a summary paragraph describing the customer relationship, their stated pain-points, and to what context/objective the report has been tailored for. <br/> 5.  Review the document, add missing pieces, fix recommendations that need to be…  <br/> 6.  Get a Solution Architect to review the document. You must get their sign-off before delivery. <br/> 7.  Refresh the table of contents.  <br/> 8.  Delete this section and the yellow box before delivery.                                                                                                      |



# Summary of findings

## Report Generation Errors
{{loadErrors}}

## Immediate issues

{{^immediate-infra.isEmpty}}
* Infrastructure
{{#immediate-infra}}
  * {{.}}
{{/immediate-infra}}{{/immediate-infra.isEmpty}}
{{^immediate-configuration.isEmpty}}
* Configuration
{{#immediate-configuration}}
  * {{.}} 
{{/immediate-configuration}}{{/immediate-configuration.isEmpty}}
{{^immediate-security.isEmpty}}
* Security
{{#immediate-security}}
  * {{.}}
{{/immediate-security}}{{/immediate-security.isEmpty}}
{{^immediate-datamodel.isEmpty}}
* Data Model
{{#immediate-datamodel}}
  * {{.}} 
{{/immediate-datamodel}}{{/immediate-datamodel.isEmpty}}
{{^immediate-operations.isEmpty}}
* Operations
{{#immediate-operations}}
  * {{.}}
{{/immediate-operations}}{{/immediate-operations.isEmpty}}
{{^immediate-misc.isEmpty}}
* Others
  {{#immediate-misc}}
  * {{.}}
{{/immediate-misc}}{{/immediate-misc.isEmpty}}

## Near term changes 

{{^nearterm-infra.isEmpty}}
* Infrastructure
  {{#nearterm-infra}}
  * {{.}}
{{/nearterm-infra}}{{/nearterm-infra.isEmpty}}
{{^nearterm-configuration.isEmpty}}
* Configuration
  {{#nearterm-configuration}}
  * {{.}}
{{/nearterm-configuration}}{{/nearterm-configuration.isEmpty}}
{{^nearterm-security.isEmpty}}
* Security
  {{#nearterm-security}}
  * {{.}}
{{/nearterm-security}}{{/nearterm-security.isEmpty}}
{{^nearterm-datamodel.isEmpty}}
* Data Model
  {{#nearterm-datamodel}}
  * {{.}}
{{/nearterm-datamodel}}{{/nearterm-datamodel.isEmpty}}
{{^nearterm-operations.isEmpty}}
* Operations
  {{#nearterm-operations}}
  * {{.}}
{{/nearterm-operations}}{{/nearterm-operations.isEmpty}}
{{^nearterm-misc.isEmpty}}
* Others
  {{#nearterm-misc}}
  * {{.}}
{{/nearterm-misc}}{{/nearterm-misc.isEmpty}}


## Long term changes 

{{^longterm-infra.isEmpty}}
* Infrastructure
  {{#longterm-infra}}
  * {{.}}
{{/longterm-infra}}{{/longterm-infra.isEmpty}}
{{^longterm-configuration.isEmpty}}
* Configuration
  {{#longterm-configuration}}
  * {{.}}
{{/longterm-configuration}}{{/longterm-configuration.isEmpty}}
{{^longterm-security.isEmpty}}
* Security
  {{#longterm-security}}
  * {{.}}
{{/longterm-security}}{{/longterm-security.isEmpty}}
{{^longterm-datamodel.isEmpty}}
* Data Model
  {{#longterm-datamodel}}
  * {{.}}
{{/longterm-datamodel}}{{/longterm-datamodel.isEmpty}}
{{^longterm-operations.isEmpty}}
* Operations
  {{#longterm-operations}}
  * {{.}}
{{/longterm-operations}}{{/longterm-operations.isEmpty}}
{{^longterm-misc.isEmpty}}
* Others
  {{#longterm-misc}}
  * {{.}}
{{/longterm-misc}}{{/longterm-misc.isEmpty}}
